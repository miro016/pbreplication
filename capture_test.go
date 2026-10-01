package pbreplication

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
)

func parallelCaptures(t *testing.T, count int, fn func(int) error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- fn(i)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentLocalSequenceAllocation(t *testing.T) {
	app, _ := newTestNode(t, "sequence-node")
	const count = 120
	sequences := make([]int64, count)
	parallelCaptures(t, count, func(i int) error {
		seq, err := incrLocalSeq(app.NonconcurrentDB())
		sequences[i] = seq
		return err
	})
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for i, seq := range sequences {
		if seq != int64(i+1) {
			t.Fatalf("sequence %d = %d, want %d; concurrent allocations must be unique and contiguous", i, seq, i+1)
		}
	}
}

func TestConcurrentCaptureConvergesOnThreeNodes(t *testing.T) {
	apps := make([]core.App, 3)
	replicators := make([]*Replicator, 3)
	for i := range apps {
		apps[i], replicators[i] = newTestNode(t, fmt.Sprintf("node-%d", i))
	}
	col := makeTestCollection(t, apps[0], "concurrent_posts")
	schema := lastOps(t, replicators[0], 1)[0]
	for _, receiver := range replicators[1:] {
		if err := receiver.applyOp(schema); err != nil {
			t.Fatal(err)
		}
	}
	collections := make([]*core.Collection, len(apps))
	for i, app := range apps {
		var err error
		collections[i], err = app.FindCollectionByNameOrId(col.Id)
		if err != nil {
			t.Fatal(err)
		}
	}

	const count = 120
	records := make([]*core.Record, count)
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			parallelCaptures(t, count, func(i int) error {
				origin := i % len(apps)
				switch operation {
				case "create":
					records[i] = core.NewRecord(collections[origin])
					records[i].Set("title", fmt.Sprintf("record-%d", i))
					return apps[origin].Save(records[i])
				case "update":
					records[i].Set("title", fmt.Sprintf("updated-%d", i))
					return apps[origin].Save(records[i])
				default:
					return apps[origin].Delete(records[i])
				}
			})

			for origin, r := range replicators {
				ops, _, err := opsAfterRowID(r.app.DB(), 0, 100000)
				if err != nil {
					t.Fatal(err)
				}
				recordOps := 0
				for i, o := range ops {
					if o.SrcSeq != int64(i+1) || o.SrcNode != r.nodeID {
						t.Fatalf("noncontiguous local oplog on %s at %d: %+v", r.nodeID, i, o)
					}
					if o.ColID == col.Id && o.Type != opColUpsert {
						recordOps++
					}
				}
				multiplier := map[string]int{"create": 1, "update": 2, "delete": 3}[operation]
				if want := count / len(apps) * multiplier; recordOps != want {
					t.Fatalf("%s captured %d record operations, want %d", r.nodeID, recordOps, want)
				}
				for peer, receiver := range replicators {
					if peer != origin {
						receiver.applyBatch(ops)
						if failed := receiver.stats.failed.Load(); failed != 0 {
							t.Fatalf("node %d failed to apply %d operations", peer, failed)
						}
					}
				}
			}

			for node, app := range apps {
				expectedCount := int64(count)
				if operation == "delete" {
					expectedCount = 0
				}
				total, err := app.CountRecords(col.Id)
				if err != nil || total != expectedCount {
					t.Fatalf("node %d records = %d, %v; want %d", node, total, err, expectedCount)
				}
				for i, record := range records {
					version, err := getVersion(app.DB(), col.Id, record.Id)
					if err != nil || version == nil || version.Deleted != (operation == "delete") {
						t.Fatalf("node %d record %s version = %+v, %v", node, record.Id, version, err)
					}
					if operation == "update" {
						saved, err := app.FindRecordById(col.Id, record.Id)
						if err != nil || saved.GetString("title") != fmt.Sprintf("updated-%d", i) {
							t.Fatalf("node %d update missing for %s: %v", node, record.Id, err)
						}
					}
				}
			}
		})
		if t.Failed() {
			return
		}
	}
}

type captureState struct {
	sequence string
	ops      int
	versions []versionRow
}

func readCaptureState(t *testing.T, app core.App) captureState {
	t.Helper()
	var state captureState
	var err error
	state.sequence, err = getState(app.DB(), stateLocalSeq)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.DB().NewQuery(`SELECT COUNT(*) FROM _repl_oplog`).Row(&state.ops); err != nil {
		t.Fatal(err)
	}
	if err := app.DB().NewQuery(`SELECT * FROM _repl_versions ORDER BY col_id, record_id`).All(&state.versions); err != nil {
		t.Fatal(err)
	}
	return state
}

func rejectVersionWrites(t *testing.T, app core.App) {
	t.Helper()
	_, err := app.DB().NewQuery(`CREATE TRIGGER reject_capture_version BEFORE INSERT ON _repl_versions
		BEGIN SELECT RAISE(ABORT, 'injected capture failure'); END`).Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func drainPushWake(r *Replicator) {
	for {
		select {
		case <-r.pushWake:
		default:
			return
		}
	}
}

func TestRecordCaptureFailureRollsBackWrite(t *testing.T) {
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			app, r := newTestNode(t, "rollback-record")
			col := makeTestCollection(t, app, "posts")
			record := core.NewRecord(col)
			record.Set("title", "original")
			if operation != "create" {
				if err := app.Save(record); err != nil {
					t.Fatal(err)
				}
			}
			before := readCaptureState(t, app)
			rejectVersionWrites(t, app)
			drainPushWake(r)
			record.Set("title", "changed")
			var err error
			if operation == "delete" {
				err = app.Delete(record)
			} else {
				err = app.Save(record)
			}
			if err == nil || !strings.Contains(err.Error(), "injected capture failure") {
				t.Fatalf("save/delete error = %v, want capture failure", err)
			}
			after := readCaptureState(t, app)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("failed capture changed replication state: before=%+v after=%+v", before, after)
			}
			count, err := app.CountRecords(col.Id)
			want := int64(1)
			if operation == "create" {
				want = 0
			}
			if err != nil || count != want {
				t.Fatalf("records = %d, %v; want %d", count, err, want)
			}
			if want == 1 {
				saved, err := app.FindRecordById(col.Id, record.Id)
				if err != nil || saved.GetString("title") != "original" {
					t.Fatalf("original record was not restored: %v", err)
				}
			}
			if len(r.pushWake) != 0 {
				t.Fatal("failed capture woke the pusher")
			}
		})
	}
}

func TestSchemaCaptureFailureRollsBackDDL(t *testing.T) {
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			app, r := newTestNode(t, "rollback-schema")
			col := core.NewBaseCollection("schema_rollback")
			col.Fields.Add(&core.TextField{Name: "title"})
			if operation != "create" {
				if err := app.Save(col); err != nil {
					t.Fatal(err)
				}
			}
			before := readCaptureState(t, app)
			rejectVersionWrites(t, app)
			drainPushWake(r)
			col.Fields.Add(&core.TextField{Name: "added"})
			var err error
			if operation == "delete" {
				err = app.Delete(col)
			} else {
				err = app.Save(col)
			}
			if err == nil || !strings.Contains(err.Error(), "injected capture failure") {
				t.Fatalf("schema save/delete error = %v, want capture failure", err)
			}
			if after := readCaptureState(t, app); !reflect.DeepEqual(before, after) {
				t.Fatalf("failed schema capture changed replication state: before=%+v after=%+v", before, after)
			}
			var tables int
			if err := app.DB().NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_rollback'`).Row(&tables); err != nil {
				t.Fatal(err)
			}
			want := 1
			if operation == "create" {
				want = 0
			}
			if tables != want {
				t.Fatalf("tables after rollback = %d, want %d", tables, want)
			}
			saved, err := app.FindCachedCollectionByNameOrId("schema_rollback")
			if want == 0 {
				if err == nil {
					t.Fatal("failed schema creation survived in the collection cache")
				}
			} else {
				if err != nil || saved.Fields.GetByName("title") == nil || saved.Fields.GetByName("added") != nil {
					t.Fatalf("original schema was not restored: collection=%v error=%v", saved, err)
				}
				var columns []struct {
					Name string `db:"name"`
				}
				if err := app.DB().NewQuery(`PRAGMA table_info(schema_rollback)`).All(&columns); err != nil {
					t.Fatal(err)
				}
				for _, column := range columns {
					if column.Name == "added" {
						t.Fatal("rolled-back column still exists physically")
					}
				}
			}
			if len(r.pushWake) != 0 {
				t.Fatal("failed schema capture woke the pusher")
			}
		})
	}
}

func TestCaptureReusesOuterTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%v", commit), func(t *testing.T) {
			app, r := newTestNode(t, "outer-tx")
			before := readCaptureState(t, app)
			drainPushWake(r)
			abort := errors.New("rollback outer transaction")
			err := app.RunInTransaction(func(txApp core.App) error {
				col := core.NewBaseCollection("outer_posts")
				if err := txApp.Save(col); err != nil {
					return err
				}
				if err := txApp.Save(core.NewRecord(col)); err != nil {
					return err
				}
				if len(r.pushWake) != 0 {
					t.Error("pusher woke before the outer transaction committed")
				}
				if !commit {
					return abort
				}
				return nil
			})
			if commit {
				if err != nil {
					t.Fatal(err)
				}
				if got := readCaptureState(t, app); got.ops != before.ops+2 {
					t.Fatalf("committed capture count = %d, want %d", got.ops, before.ops+2)
				}
				if len(r.pushWake) == 0 {
					t.Fatal("committed transaction didn't wake the pusher")
				}
			} else {
				if !errors.Is(err, abort) {
					t.Fatalf("outer transaction error = %v", err)
				}
				if after := readCaptureState(t, app); !reflect.DeepEqual(before, after) {
					t.Fatalf("outer rollback retained capture: %+v", after)
				}
				if len(r.pushWake) != 0 {
					t.Fatal("rolled-back outer transaction woke the pusher")
				}
			}
		})
	}
}

func TestSchemaDDLFailureRollsBackCapture(t *testing.T) {
	app, r := newTestNode(t, "ddl-rollback")
	col := makeTestCollection(t, app, "posts")
	for range 2 {
		record := core.NewRecord(col)
		record.Set("title", "duplicate")
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}
	}
	before := readCaptureState(t, app)
	drainPushWake(r)
	captured := false
	app.OnCollectionUpdateExecute().Bind(&hook.Handler[*core.CollectionEvent]{
		Priority: hookPriority - 1,
		Func: func(e *core.CollectionEvent) error {
			if err := e.Next(); err != nil {
				return err
			}
			captured = readCaptureState(t, e.App).ops == before.ops+1
			return nil
		},
	})
	col.Indexes = append(col.Indexes, `CREATE UNIQUE INDEX idx_capture_unique_title ON posts (title)`)
	err := app.Save(col)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("expected unique-index DDL failure, got %v", err)
	}
	if !captured {
		t.Fatal("schema capture did not run before the DDL failure")
	}
	if after := readCaptureState(t, app); !reflect.DeepEqual(before, after) {
		t.Fatalf("DDL failure retained capture: before=%+v after=%+v", before, after)
	}
	if len(r.pushWake) != 0 {
		t.Fatal("failed DDL woke the pusher")
	}
}

func TestLocalSequenceCollisionFailsButRemoteRetryIsIdempotent(t *testing.T) {
	app, r := newTestNode(t, "collision-node")
	col := makeTestCollection(t, app, "posts")
	record := core.NewRecord(col)
	if err := app.Save(record); err != nil {
		t.Fatal(err)
	}
	original := lastOps(t, r, 1)[0]
	if err := setState(app.DB(), stateLocalSeq, fmt.Sprint(original.SrcSeq-1)); err != nil {
		t.Fatal(err)
	}
	before := readCaptureState(t, app)
	err := app.Save(core.NewRecord(col))
	if err == nil {
		t.Fatal("a local oplog sequence collision was silently acknowledged")
	}
	if after := readCaptureState(t, app); !reflect.DeepEqual(before, after) {
		t.Fatalf("sequence collision changed replication state: before=%+v after=%+v", before, after)
	}
	total, err := app.CountRecords(col.Id)
	if err != nil || total != 1 {
		t.Fatalf("collision persisted a record: total=%d error=%v", total, err)
	}
	inserted, err := insertOpIfAbsent(app.DB(), original)
	if err != nil || inserted {
		t.Fatalf("remote retry must remain idempotent: inserted=%v error=%v", inserted, err)
	}
	var stored string
	if err := app.DB().NewQuery(`SELECT record_id FROM _repl_oplog WHERE src_node={:node} AND src_seq={:seq}`).
		Bind(dbx.Params{"node": original.SrcNode, "seq": original.SrcSeq}).Row(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != record.Id {
		t.Fatalf("original operation was replaced: record=%s", stored)
	}
}
