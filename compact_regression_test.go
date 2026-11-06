package pbreplication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func remoteUpsert(r *Replicator, col *core.Collection, src string, seq int64, recordID, title string) *op {
	return &op{
		SrcNode: src, SrcSeq: seq, HLC: r.clock.Now(), Type: opUpsert,
		ColID: col.Id, ColName: col.Name, RecordID: recordID,
		Payload: []byte(fmt.Sprintf(`{"id":%q,"title":%q}`, recordID, title)),
	}
}

func countOpsWhere(t *testing.T, app core.App, where string, params dbx.Params) int {
	t.Helper()
	var n int
	if err := app.DB().NewQuery("SELECT COUNT(*) FROM _repl_oplog WHERE " + where).Bind(params).Row(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Regression: the snapshot horizon of a source must be the highest dropped
// delete, not the source's current head. A merely lagging peer must keep
// receiving ordinary operations.
func TestCompactionHorizonIsDroppedDeleteNotSourceHead(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	col := makeTestCollection(t, app, "gc_horizon")
	old := saveTestRecord(t, app, col.Name, "expires")
	if err := app.Delete(old); err != nil {
		t.Fatal(err)
	}
	delSeq := lastOps(t, r, 1)[0].SrcSeq
	ageGCState(t, app)
	for i := 0; i < 5; i++ {
		saveTestRecord(t, app, col.Name, "fresh")
	}
	completeGC(t, r)
	if v, _ := getVersion(app.DB(), col.Id, old.Id); v != nil {
		t.Fatalf("expired tombstone was not collected: %+v", v)
	}
	var horizon int64
	if err := app.DB().NewQuery("SELECT min_seq FROM _repl_compaction WHERE src_node = {:s}").
		Bind(dbx.Params{"s": r.nodeID}).Row(&horizon); err != nil {
		t.Fatal(err)
	}
	if horizon != delSeq {
		t.Fatalf("snapshot horizon = %d, want dropped delete seq %d", horizon, delSeq)
	}
	ops, snap, err := opsAfterVector(app.DB(), map[string]int64{r.nodeID: delSeq}, 100)
	if err != nil || snap || len(ops) != 5 {
		t.Fatalf("lagging peer that saw the delete: ops=%d snapshot=%v err=%v", len(ops), snap, err)
	}
	if _, snap, err := opsAfterVector(app.DB(), map[string]int64{r.nodeID: delSeq - 1}, 100); err != nil || !snap {
		t.Fatalf("peer missing the dropped delete must resync: snapshot=%v err=%v", snap, err)
	}
	var pseudo int
	_ = app.DB().NewQuery("SELECT COUNT(*) FROM _repl_compaction WHERE src_node != {:s}").
		Bind(dbx.Params{"s": r.nodeID}).Row(&pseudo)
	if pseudo != 0 {
		t.Fatalf("version purge recorded %d unexpected horizons", pseudo)
	}
}

// Regression: dropping superseded operations must not fence a delayed,
// out-of-order create from the same source.
func TestCompactionOutOfOrderCreateAdmittedAfterSupersededGC(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_out_of_order")
	db := app.NonconcurrentDB()
	createY := remoteUpsert(r, col, "remoteS", 1, "yyyyyyyyyyyyyy1", "Y") // generated first
	upd1 := remoteUpsert(r, col, "remoteS", 2, "xxxxxxxxxxxxxx1", "X1")
	upd2 := remoteUpsert(r, col, "remoteS", 3, "xxxxxxxxxxxxxx1", "X2")
	for _, o := range []*op{upd1, upd2} {
		if ok, err := insertOpIfAbsent(db, o); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if err := r.applyOp(o); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-2 * r.cfg.CompactionInterval).UTC().Format(time.RFC3339)
	if _, err := db.NewQuery("UPDATE _repl_oplog SET created = {:c}").Bind(dbx.Params{"c": past}).Execute(); err != nil {
		t.Fatal(err)
	}
	if s := completeGC(t, r); s.Deleted != 1 {
		t.Fatalf("superseded op not collected: %+v", s)
	}
	ok, err := insertOpIfAbsent(db, createY)
	if err != nil || !ok {
		t.Fatalf("delayed legitimate create refused after superseded GC: %v %v", ok, err)
	}
	if err := r.applyOp(createY); err != nil {
		t.Fatal(err)
	}
	if _, err := app.FindRecordById(col, createY.RecordID); err != nil {
		t.Fatalf("delayed create was not applied: %v", err)
	}
}

// Regression: records of a deleted collection, or of an earlier incarnation
// of a collection recreated with the same id, are not "repaired" into
// replicated deletes.
func TestCompactionNoRepairForDeletedOrRecreatedCollection(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_dropped")
	colID := col.Id
	for i := 0; i < 5; i++ {
		saveTestRecord(t, app, col.Name, "row")
	}
	if err := app.Delete(col); err != nil {
		t.Fatal(err)
	}
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("deleted collection produced %d repairs", s.Repaired)
	}
	// The record versions predate the recreated collection.
	hourAgo := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if _, err := app.NonconcurrentDB().NewQuery("UPDATE _repl_versions SET updated = {:u} WHERE col_id = {:c}").
		Bind(dbx.Params{"u": hourAgo, "c": colID}).Execute(); err != nil {
		t.Fatal(err)
	}
	again := core.NewBaseCollection("gc_dropped")
	again.Id = colID
	again.Fields.Add(&core.TextField{Name: "title"})
	if err := app.Save(again); err != nil {
		t.Fatal(err)
	}
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("recreated collection produced %d repairs for an earlier incarnation", s.Repaired)
	}
	if n := countOpsWhere(t, app, "op_type = 'delete' AND col_id = {:c}", dbx.Params{"c": colID}); n != 0 {
		t.Fatalf("%d replicated record deletes emitted", n)
	}
}

// Regression: a remote create that never applied locally (unknown
// collection, or a failed apply) is not evidence of a deletion.
func TestCompactionNoRepairForUnappliedRemoteCreate(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_unapplied")
	unknown := &op{SrcNode: "remoteS", SrcSeq: 1, HLC: r.clock.Now(), Type: opUpsert,
		ColID: "pbc_unknown00001", ColName: "not_here", RecordID: "livepeerrecord1", Payload: []byte(`{"id":"livepeerrecord1"}`)}
	failed := remoteUpsert(r, col, "remoteS", 2, "failedapply0001", "never applied here")
	for _, o := range []*op{unknown, failed} {
		if ok, err := insertOpIfAbsent(app.NonconcurrentDB(), o); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	r.applyRecordBatch([]*op{unknown}) // parks: collection unknown
	ageGCState(t, app)
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("unapplied remote creates produced %d repairs", s.Repaired)
	}
	if n := countOpsWhere(t, app, "src_node = {:s}", dbx.Params{"s": r.nodeID}); n != 1 { // the collection create
		t.Fatalf("GC emitted local operations for unapplied remote creates: %d", n)
	}
}

// Regression: a record applied through name resolution (the remote
// collection id differs) stays live.
func TestCompactionNoRepairForNameResolvedCollection(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_same_name")
	o := &op{SrcNode: "remoteS", SrcSeq: 1, HLC: r.clock.Now(), Type: opUpsert,
		ColID: "pbc_remoteid0001", ColName: col.Name, RecordID: "sharedrecord001",
		Payload: []byte(`{"id":"sharedrecord001","title":"live"}`)}
	if ok, err := insertOpIfAbsent(app.NonconcurrentDB(), o); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := r.applyOp(o); err != nil {
		t.Fatal(err)
	}
	ageGCState(t, app)
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("live name-resolved record repaired: %+v", s)
	}
	if v, _ := getVersion(app.DB(), o.ColID, o.RecordID); v == nil || v.Deleted {
		t.Fatalf("live LWW state changed: %+v", v)
	}
}

// Regression: local operations bypass the remote admission fence; rescued
// creates behind the fence are skipped consistently with peers.
func TestLocalOpsBypassFenceAndRescueSkipsFencedCreates(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_rescue")
	existing := saveTestRecord(t, app, col.Name, "before offline update")
	oldHLC := r.clock.Now()
	time.Sleep(2 * time.Millisecond)
	if err := setState(app.NonconcurrentDB(), stateGCHLC, r.clock.Now()); err != nil {
		t.Fatal(err)
	}
	db := app.NonconcurrentDB()

	seq, err := incrLocalSeq(db)
	if err != nil {
		t.Fatal(err)
	}
	local := &op{SrcNode: r.nodeID, SrcSeq: seq, HLC: oldHLC, Type: opUpsert, ColID: col.Id, ColName: col.Name,
		RecordID: "localreemit0001", Payload: []byte(`{}`)}
	if err := insertOp(db, local); err != nil {
		t.Fatalf("local re-emit refused: %v", err)
	}
	remote := &op{SrcNode: "remoteS", SrcSeq: 1, HLC: oldHLC, Type: opUpsert, ColID: col.Id, ColName: col.Name,
		RecordID: "remotefenced001", Payload: []byte(`{}`)}
	if ok, err := insertOpIfAbsent(db, remote); err != nil || ok {
		t.Fatalf("remote op behind fence admitted: %v %v", ok, err)
	}
	if fenced, err := opFenced(db, remote); err != nil || !fenced {
		t.Fatalf("fenced op not classified: %v %v", fenced, err)
	}
	r.noteFencedOp(remote)
	if r.stats.fenced.Load() != 1 {
		t.Fatal("fence refusal not counted")
	}

	rescuedCreate := &op{SrcNode: r.nodeID, SrcSeq: 99, HLC: oldHLC, Type: opUpsert, ColID: col.Id, ColName: col.Name,
		RecordID: "offlinecreate01", Payload: []byte(`{"id":"offlinecreate01","title":"offline"}`)}
	head, _ := maxRowID(app.DB())
	if ok, err := r.replayRescuedOp(rescuedCreate); ok || !errors.Is(err, errRescueFenced) {
		t.Fatalf("rescued create behind fence: ok=%v err=%v", ok, err)
	}
	if _, err := app.FindRecordById(col, rescuedCreate.RecordID); err == nil {
		t.Fatal("fenced rescued create applied locally")
	}
	if now, _ := maxRowID(app.DB()); now != head {
		t.Fatal("fenced rescued create re-emitted")
	}
	// Existing LWW state is not fenced: a newer rescued update replays.
	cur, _ := getVersion(app.DB(), col.Id, existing.Id)
	rescuedUpdate := &op{SrcNode: r.nodeID, SrcSeq: 100, HLC: oldHLC, Type: opUpsert, ColID: col.Id, ColName: col.Name,
		RecordID: existing.Id, Payload: []byte(fmt.Sprintf(`{"id":%q,"title":"offline update"}`, existing.Id))}
	if cur == nil || !supersedes(rescuedUpdate, cur) {
		t.Fatalf("fixture: rescued update must win LWW: %+v", cur)
	}
	if ok, err := r.replayRescuedOp(rescuedUpdate); err != nil || !ok {
		t.Fatalf("rescued update of existing record: ok=%v err=%v", ok, err)
	}
}

// The small tables are visited while a large oplog phase is still running.
func TestCompactionSmallPhasesNotStarvedByOplog(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches = 2, 1
	col := makeTestCollection(t, app, "gc_fair")
	rec := saveTestRecord(t, app, col.Name, "v0")
	for i := 0; i < 20; i++ {
		rec.Set("title", fmt.Sprint(i))
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := app.NonconcurrentDB().NewQuery(`INSERT INTO _repl_members (node_id, last_seen)
		VALUES ('stale', '2000-01-01T00:00:00Z')`).Execute(); err != nil {
		t.Fatal(err)
	}
	ageGCState(t, app)
	for i := 0; i < 4; i++ {
		if _, err := r.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := getMember(app.DB(), "stale")
	if m == nil || !m.Removed {
		t.Fatalf("members phase starved behind the oplog phase: %+v", m)
	}
	raw, _ := getState(app.DB(), stateGCPass)
	if raw == "" {
		t.Fatal("expected the oplog phase to be unfinished")
	}
	completeGC(t, r)
}

// A v0.7.3 database (no rowid high-water mark, trigger or client GC
// indexes) upgrades in place, idempotently, without rowid reuse.
func TestStorageUpgradeFromV073Schema(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_upgrade")
	for i := 0; i < 3; i++ {
		saveTestRecord(t, app, col.Name, "pre-upgrade")
	}
	db := app.NonconcurrentDB()
	for _, q := range []string{
		"DROP TRIGGER _repl_oplog_rowid",
		"DROP INDEX _repl_client_paths_gc_idx",
		"DROP INDEX _repl_client_ips_gc_idx",
		"DELETE FROM _repl_state WHERE key IN ('oplog_rowid', 'gc_pass', 'gc_hlc')",
	} {
		if _, err := db.NewQuery(q).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := maxRowID(app.DB())
	for i := 0; i < 2; i++ {
		if err := createTables(app); err != nil {
			t.Fatalf("upgrade run %d: %v", i, err)
		}
	}
	if mark, _ := getState(app.DB(), "oplog_rowid"); mark != fmt.Sprint(head) {
		t.Fatalf("high-water mark = %q, want %d", mark, head)
	}
	var objects int
	_ = app.DB().NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE name IN
		('_repl_oplog_rowid', '_repl_client_paths_gc_idx', '_repl_client_ips_gc_idx')`).Row(&objects)
	if objects != 3 {
		t.Fatalf("upgrade objects = %d", objects)
	}
	var oplogIdx int
	_ = app.DB().NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND tbl_name = '_repl_oplog'
		AND name NOT LIKE 'sqlite_autoindex%'`).Row(&oplogIdx)
	if oplogIdx != 2 {
		t.Fatalf("upgrade must not build new oplog indexes, found %d", oplogIdx)
	}
	completeGC(t, r)
	saveTestRecord(t, app, col.Name, "post-upgrade")
	if ops, last, err := opsAfterRowID(app.DB(), head, 10); err != nil || len(ops) != 1 || last <= head {
		t.Fatalf("post-upgrade write not after the old head: %d %d %v", len(ops), last, err)
	}
}

// A downgraded (v0.7.3) binary inserts without explicit rowids; the trigger
// keeps the high-water mark monotonic so a re-upgrade never reuses rowids.
func TestDowngradedInsertKeepsRowIDHighWaterMark(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_downgrade")
	if err := app.Delete(col); err != nil {
		t.Fatal(err)
	}
	high, _ := maxRowID(app.DB())
	ageGCState(t, app)
	completeGC(t, r)
	if head, _ := maxRowID(app.DB()); head != 0 {
		t.Fatalf("fixture expects an empty oplog, head=%d", head)
	}
	if _, err := app.NonconcurrentDB().NewQuery(`INSERT INTO _repl_oplog
		(src_node, src_seq, hlc, op_type, col_id, col_name, record_id, payload, files, created)
		VALUES ('old-binary', 1, '0', 'upsert', 'c', 'c', 'r', '', '', '2026-01-01T00:00:00Z')`).Execute(); err != nil {
		t.Fatalf("v0.7.3-style insert failed with the trigger present: %v", err)
	}
	var mark int64
	if err := app.DB().NewQuery("SELECT CAST(value AS INTEGER) FROM _repl_state WHERE key = 'oplog_rowid'").Row(&mark); err != nil || mark < high {
		t.Fatalf("high-water mark regressed: %d < %d (%v)", mark, high, err)
	}
	saveTestRecord(t, app, makeTestCollection(t, app, "gc_reupgrade").Name, "after re-upgrade")
	if head, _ := maxRowID(app.DB()); head <= mark {
		t.Fatalf("re-upgraded insert reused a rowid: %d <= %d", head, mark)
	}
}

// Concurrent captured writes, deletes, remote ingest/apply and bounded GC
// never fail, never repair live state and never lose a live record.
func TestCompactionConcurrentWritesIngestAndGC(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.CompactionInterval = time.Nanosecond
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches = 5, 4
	col := makeTestCollection(t, app, "gc_concurrent")
	stop := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	var liveMu sync.Mutex
	live := map[string]bool{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := r.Compact(context.Background()); err != nil && !errors.Is(err, ErrCompactionRunning) {
				errs <- fmt.Errorf("compact: %w", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			rec := core.NewRecord(col)
			rec.Set("title", "local")
			if err := app.Save(rec); err != nil {
				errs <- err
				return
			}
			rec.Set("title", "updated")
			if err := app.Save(rec); err != nil {
				errs <- err
				return
			}
			if i%3 == 0 {
				if err := app.Delete(rec); err != nil {
					errs <- err
					return
				}
				continue
			}
			liveMu.Lock()
			live[rec.Id] = true
			liveMu.Unlock()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 60; i++ {
			id := fmt.Sprintf("remoterec%06d", i)
			o := remoteUpsert(r, col, "remoteS", int64(i), id, "remote")
			if ok, err := insertOpIfAbsent(app.NonconcurrentDB(), o); err != nil || !ok {
				errs <- fmt.Errorf("ingest %d: %v %v", i, ok, err)
				return
			}
			if err := r.applyOp(o); err != nil {
				errs <- err
				return
			}
			liveMu.Lock()
			live[id] = true
			liveMu.Unlock()
		}
	}()
	time.Sleep(10 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		// let writers finish, then stop the collector
		for {
			liveMu.Lock()
			n := len(live)
			liveMu.Unlock()
			if n >= 100 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		close(done)
	}()
	select {
	case <-done:
	case err := <-errs:
		close(stop)
		wg.Wait()
		t.Fatal(err)
	case <-time.After(2 * time.Minute):
		t.Fatal("concurrent workload timed out")
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("live state repaired after concurrent workload: %+v", s)
	}
	for id := range live {
		if _, err := app.FindRecordById(col, id); err != nil {
			t.Fatalf("live record %s lost: %v", id, err)
		}
		if v, _ := getVersion(app.DB(), col.Id, id); v == nil || v.Deleted {
			t.Fatalf("live record %s lost its version: %+v", id, v)
		}
	}
}

// TestCompactionThroughputFixture measures collector throughput on a larger
// synthetic history. It is not a production benchmark and only runs with
// PBR_GC_BENCH=1:
//
//	PBR_GC_BENCH=1 go test -run TestCompactionThroughputFixture -v .
func TestCompactionThroughputFixture(t *testing.T) {
	if os.Getenv("PBR_GC_BENCH") == "" {
		t.Skip("set PBR_GC_BENCH=1 to run the GC throughput fixture")
	}
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	r.cfg.CompactionInterval = time.Minute
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches, r.cfg.CompactionTimeBudget = 200, 128, 2*time.Second
	col := makeTestCollection(t, app, "gc_perf")
	const records, updates = 20000, 3
	if err := app.RunInTransaction(func(tx core.App) error {
		c, err := tx.FindCollectionByNameOrId(col.Name)
		if err != nil {
			return err
		}
		for i := 0; i < records; i++ {
			rec := core.NewRecord(c)
			rec.Set("title", "a")
			if err := tx.Save(rec); err != nil {
				return err
			}
			for u := 0; u < updates; u++ {
				rec.Set("title", fmt.Sprint(u))
				if err := tx.Save(rec); err != nil {
					return err
				}
			}
			if i%4 == 0 {
				if err := tx.Delete(rec); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ageGCState(t, app)
	var oplog, versions int
	_ = app.DB().NewQuery("SELECT COUNT(*) FROM _repl_oplog").Row(&oplog)
	_ = app.DB().NewQuery("SELECT COUNT(*) FROM _repl_versions").Row(&versions)
	measure := func(label string) {
		start := time.Now()
		var runs, scanned, deleted, maxRun int
		for {
			s, err := r.Compact(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			runs++
			scanned += s.Scanned
			deleted += s.Deleted
			if s.Scanned > maxRun {
				maxRun = s.Scanned
			}
			if s.Complete {
				break
			}
		}
		elapsed := time.Since(start)
		t.Logf("%s: runs=%d scanned=%d deleted=%d elapsed=%s rate=%.0f rows/s max_rows_per_2s_run=%d",
			label, runs, scanned, deleted, elapsed.Round(time.Millisecond), float64(scanned)/elapsed.Seconds(), maxRun)
	}
	t.Logf("fixture: oplog=%d versions=%d", oplog, versions)
	measure("backlog pass")
	measure("steady rescan pass")
}

// End to end over HTTP through the real pull handler: after the source
// collects an expired tombstone, a peer that already holds the delete but
// lags on newer writes keeps replicating normally, while a peer that never
// saw the delete is told to resync.
func TestPullAfterGCLaggingPeerReplicatesWithoutResync(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	rA.cfg.TombstoneRetention = 7 * 24 * time.Hour
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/pull", func(w http.ResponseWriter, req *http.Request) {
		ev := &core.RequestEvent{App: appA}
		ev.Request = req
		ev.Response = w
		if err := rA.handlePull(ev); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	peerA := func() *member { return &member{NodeID: rA.nodeID, URL: srv.URL} }
	newPeer := func(id string) (*tests.TestApp, *Replicator) {
		app := newTestAppOnly(t)
		return app, newTestNodeCfg(t, app, Config{
			NodeID: id, NodeURL: "http://" + id + ".test:8090",
			ClusterSecret: testSecret, ResyncStrategy: "restart-copy",
		})
	}
	pull := func(r *Replicator) { // pull, then apply what was queued (no apply loop in tests)
		t.Helper()
		if err := r.pullFromPeer(peerA()); err != nil {
			t.Fatal(err)
		}
		for {
			select {
			case o := <-r.applyCh:
				r.applyBatch(r.drainApplyQueue(o))
			default:
				return
			}
		}
	}
	resyncFlagged := func(r *Replicator) bool {
		pending, err := getState(r.app.DB(), stateResyncPending)
		if err != nil {
			t.Fatal(err)
		}
		return pending != ""
	}

	col := makeTestCollection(t, appA, "gc_http")
	keep := saveTestRecord(t, appA, col.Name, "keep")
	gone := saveTestRecord(t, appA, col.Name, "gone")
	if err := appA.Delete(gone); err != nil {
		t.Fatal(err)
	}
	appB, rB := newPeer("nodeB0000000001")
	pull(rB)
	if _, err := appB.FindRecordById(col.Name, keep.Id); err != nil {
		t.Fatalf("initial pull: %v", err)
	}

	ageGCState(t, appA)
	fresh := make([]*core.Record, 0, 5)
	for i := 0; i < 5; i++ {
		fresh = append(fresh, saveTestRecord(t, appA, col.Name, fmt.Sprintf("fresh-%d", i)))
	}
	completeGC(t, rA)
	if v, _ := getVersion(appA.DB(), col.Id, gone.Id); v != nil {
		t.Fatalf("expired tombstone was not collected on the source: %+v", v)
	}

	pull(rB)
	if resyncFlagged(rB) {
		t.Fatal("lagging peer that already holds the dropped delete was forced to resync")
	}
	for _, rec := range fresh {
		if _, err := appB.FindRecordById(col.Name, rec.Id); err != nil {
			t.Fatalf("lagging peer missed %s after GC: %v", rec.Id, err)
		}
	}
	if _, err := appB.FindRecordById(col.Name, gone.Id); err == nil {
		t.Fatal("deleted record resurrected on the lagging peer")
	}

	_, rC := newPeer("nodeC0000000001")
	pull(rC)
	if !resyncFlagged(rC) {
		t.Fatal("peer that never saw the dropped delete was not told to resync")
	}
}
