package pbreplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

func completeGC(t *testing.T, r *Replicator) CompactionStats {
	t.Helper()
	var total CompactionStats
	for i := 0; i < 200; i++ {
		s, err := r.Compact(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if s.Batches > r.cfg.CompactionMaxBatches || s.Scanned > s.Batches*r.cfg.CompactionBatchSize {
			t.Fatalf("unbounded batch: %+v", s)
		}
		total.Batches += s.Batches
		total.Scanned += s.Scanned
		total.Deleted += s.Deleted
		total.Repaired += s.Repaired
		if s.Complete {
			total.Complete = true
			return total
		}
	}
	t.Fatal("GC did not converge")
	return total
}

// ageGCState moves all replication metadata, and the collections it
// belongs to, far into the past (collections first, so orphan repair still
// sees the evidence as belonging to the current collection incarnation).
func ageGCState(t *testing.T, app core.App) {
	t.Helper()
	for _, q := range []string{
		"UPDATE _collections SET created = '1999-01-01 00:00:00.000Z'",
		"UPDATE _repl_oplog SET created = '2000-01-01T00:00:00Z'",
		"UPDATE _repl_versions SET updated = '2000-01-01T00:00:00Z'",
	} {
		if _, err := app.NonconcurrentDB().NewQuery(q).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.ReloadCachedCollections(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionBoundedResumeAndLiveLWW(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches = 2, 1
	col := makeTestCollection(t, app, "gc_live")
	for i := 0; i < 7; i++ {
		rec := saveTestRecord(t, app, col.Name, "old")
		rec.Set("title", "live")
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
	}
	ageGCState(t, app)
	var before []versionRow
	if err := app.DB().NewQuery("SELECT * FROM _repl_versions ORDER BY col_id, record_id").All(&before); err != nil {
		t.Fatal(err)
	}
	s, err := r.Compact(context.Background())
	if err != nil || s.Complete || s.Batches != 1 || s.Scanned != 2 {
		t.Fatalf("first bounded run: %+v, %v", s, err)
	}
	progress, _ := getState(app.DB(), stateGCPass)
	if progress == "" {
		t.Fatal("resume cursor was not persisted")
	}
	// No writer is held between invocations; fresh captured writes still work.
	extra := saveTestRecord(t, app, col.Name, "between batches")
	// A new controller instance has no in-memory GC cursor to depend on.
	resumed := &Replicator{app: app, cfg: r.cfg, nodeID: r.nodeID, clock: r.clock, pushWake: r.pushWake, stopCh: r.stopCh, excluded: r.excluded, opFailLast: map[string]time.Time{}}
	resumed.ready.Store(true)
	total := completeGC(t, resumed)
	if total.Batches < 4 {
		t.Fatalf("expected multiple resumable batches: %+v", total)
	}
	var count int
	if err := app.DB().NewQuery("SELECT COUNT(*) FROM _repl_oplog WHERE col_id = {:c} AND op_type = 'upsert'").
		Bind(dbx.Params{"c": col.Id}).Row(&count); err != nil || count != 8 {
		t.Fatalf("latest ops not bounded by live records: %d, %v", count, err)
	}
	for _, v := range before {
		got, err := getVersion(app.DB(), v.ColID, v.RecordID)
		if err != nil || got == nil || *got != v {
			t.Fatalf("old live LWW state changed: %+v -> %+v (%v)", v, got, err)
		}
	}
	if _, err := app.FindRecordById(col, extra.Id); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionContinuedInsertsDoNotStarvePassWrap(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches = 2, 1
	col := makeTestCollection(t, app, "gc_continued_writes")
	var rec *core.Record
	for i := 0; i < 10; i++ {
		rec = saveTestRecord(t, app, col.Name, "initial")
		rec.Set("title", "updated")
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
	}
	ageGCState(t, app)
	first, err := r.Compact(context.Background())
	if err != nil || first.Complete {
		t.Fatalf("expected unfinished initial pass: %+v %v", first, err)
	}
	var deferredSeq int64
	completed := false
	for i := 0; i < 100; i++ {
		rec.Set("title", fmt.Sprintf("continued-%d", i))
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			deferredSeq = lastOps(t, r, 1)[0].SrcSeq
			if _, err := app.NonconcurrentDB().NewQuery(`UPDATE _repl_oplog SET created = '2000-01-01T00:00:00Z'
				WHERE src_node = {:s} AND src_seq = {:q}`).
				Bind(dbx.Params{"s": r.nodeID, "q": deferredSeq}).Execute(); err != nil {
				t.Fatal(err)
			}
		}
		s, err := r.Compact(context.Background())
		if err != nil || s.Batches > 1 || s.Scanned > 2 {
			t.Fatalf("continued-write batch exceeded bounds: %+v %v", s, err)
		}
		if s.Complete {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("continued inserts prevented the finite-head pass from completing")
	}
	progress, err := getState(app.DB(), stateGCPass)
	if err != nil || progress != "" {
		t.Fatalf("completed pass did not reset its durable cursor: %q %v", progress, err)
	}
	var count int
	countDeferred := func() int {
		t.Helper()
		if err := app.DB().NewQuery("SELECT COUNT(*) FROM _repl_oplog WHERE src_node = {:s} AND src_seq = {:q}").
			Bind(dbx.Params{"s": r.nodeID, "q": deferredSeq}).Row(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if countDeferred() != 1 {
		t.Fatal("new row above the captured oplog head was not deferred to the next pass")
	}
	completeGC(t, r)
	if countDeferred() != 0 {
		t.Fatal("wrapped cursor did not reclaim the deferred superseded operation")
	}
	got, err := app.FindRecordById(col, rec.Id)
	if err != nil || got.GetString("title") != rec.GetString("title") {
		t.Fatalf("continued writes were lost: %+v %v", got, err)
	}
}

func TestCompactionSightTuningProfiles(t *testing.T) {
	for _, profile := range []struct {
		name       string
		interval   time.Duration
		maxBatches int
	}{
		{"steady", time.Minute, 128},
		{"catch-up", 10 * time.Second, 256},
	} {
		t.Run(profile.name, func(t *testing.T) {
			app, r := newTestNode(t, "nodeA0000000001")
			r.cfg.CompactionInterval = profile.interval
			r.cfg.CompactionBatchSize = 200
			r.cfg.CompactionMaxBatches = profile.maxBatches
			r.cfg.CompactionTimeBudget = 2 * time.Second
			r.cfg.TombstoneRetention = 7 * 24 * time.Hour
			col := makeTestCollection(t, app, "gc_tuning")
			rec := saveTestRecord(t, app, col.Name, "initial")
			if err := app.RunInTransaction(func(txApp core.App) error {
				for i := 0; i < 700; i++ {
					rec.Set("title", fmt.Sprintf("updated-%d", i))
					if err := txApp.Save(rec); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			ageGCState(t, app)
			start := time.Now()
			stats := completeGC(t, r)
			if stats.Deleted != 700 {
				t.Fatalf("tuning profile did not converge: %+v", stats)
			}
			t.Logf("%s fixture: scanned=%d deleted=%d batches=%d duration=%s (not a production throughput benchmark)",
				profile.name, stats.Scanned, stats.Deleted, stats.Batches, time.Since(start))
		})
	}
}

func TestCompactionSevenDayTombstonesAndStaleReplay(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	col := makeTestCollection(t, app, "gc_deleted")
	old := saveTestRecord(t, app, col.Name, "expired")
	oldOp := *lastOps(t, r, 1)[0]
	if err := app.Delete(old); err != nil {
		t.Fatal(err)
	}
	ageGCState(t, app)
	fresh := saveTestRecord(t, app, col.Name, "fresh")
	if err := app.Delete(fresh); err != nil {
		t.Fatal(err)
	}
	completeGC(t, r)
	if v, err := getVersion(app.DB(), col.Id, old.Id); err != nil || v != nil {
		t.Fatalf("expired tombstone retained: %+v %v", v, err)
	}
	if v, err := getVersion(app.DB(), col.Id, fresh.Id); err != nil || v == nil || !v.Deleted {
		t.Fatalf("seven-day tombstone lost: %+v %v", v, err)
	}
	if _, snap, err := opsAfterVector(app.DB(), map[string]int64{}, 100); err != nil || !snap {
		t.Fatalf("lagging peer did not require snapshot: %v %v", snap, err)
	}
	oldOp.SrcNode = "offline-source"
	// A different source cannot replay old state after the version expired.
	if inserted, err := insertOpIfAbsent(app.NonconcurrentDB(), &oldOp); err != nil || inserted {
		t.Fatalf("expired replay admitted: %v %v", inserted, err)
	}
	if err := r.applyRecordOp(&oldOp); err != nil {
		t.Fatal(err)
	}
	r.applyRecordBatch([]*op{&oldOp})
	if _, err := app.FindRecordById(col, old.Id); err == nil {
		t.Fatal("stale operation resurrected an expired record")
	}
	// An authoritative logical snapshot may restore old live state.
	oldOp.SrcSeq = 0
	if err := r.applyRecordOp(&oldOp); err != nil {
		t.Fatal(err)
	}
	if _, err := app.FindRecordById(col, old.Id); err != nil {
		t.Fatalf("snapshot recovery blocked by GC: %v", err)
	}
}

func TestCompactionRepairsLegacyOrphans(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	r.cfg.CompactionBatchSize, r.cfg.CompactionMaxBatches = 2, 1
	col := makeTestCollection(t, app, "gc_orphans")
	live := saveTestRecord(t, app, col.Name, "live")
	var ids []string
	for i := 0; i < 4; i++ {
		rec := saveTestRecord(t, app, col.Name, "raw-deleted")
		ids = append(ids, rec.Id)
		if _, err := app.NonconcurrentDB().NewQuery("DELETE FROM {{" + col.Name + "}} WHERE id = {:id}").
			Bind(dbx.Params{"id": rec.Id}).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	// Cover log-only and version-only legacy leftovers as well as both.
	for i, table := range []string{"_repl_versions", "_repl_oplog"} {
		if _, err := app.NonconcurrentDB().NewQuery("DELETE FROM " + table + " WHERE record_id = {:id}").
			Bind(dbx.Params{"id": ids[i]}).Execute(); err != nil {
			t.Fatal(err)
		}
		// Prior retention can also leave a deleted version but only an upsert log.
		if _, err := app.NonconcurrentDB().NewQuery("UPDATE _repl_versions SET deleted = 1 WHERE record_id = {:id}").
			Bind(dbx.Params{"id": ids[3]}).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	ageGCState(t, app)
	before, _ := getState(app.DB(), stateLocalSeq)
	s := completeGC(t, r)
	if s.Repaired != 4 {
		t.Fatalf("orphan repairs = %d, want 4", s.Repaired)
	}
	freshCutoff := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	for _, id := range ids {
		v, err := getVersion(app.DB(), col.Id, id)
		if err != nil || v == nil || !v.Deleted || v.SrcNode != r.nodeID || v.Updated < freshCutoff {
			t.Fatalf("missing fresh repair tombstone: %+v %v", v, err)
		}
	}
	after, _ := getState(app.DB(), stateLocalSeq)
	if before == after {
		t.Fatal("orphan repair did not allocate local source sequences")
	}
	if v, _ := getVersion(app.DB(), col.Id, live.Id); v == nil || v.Deleted {
		t.Fatal("live record was treated as orphan")
	}
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("repair was not idempotent: %+v", s)
	}
	// Once the full configured retention elapses, metadata converges away.
	ageGCState(t, app)
	completeGC(t, r)
	for _, id := range ids {
		if v, _ := getVersion(app.DB(), col.Id, id); v != nil {
			t.Fatalf("expired repair metadata remains: %+v", v)
		}
	}
}

func TestCompactionEmptyOplogCursorAndRollback(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.TombstoneRetention = 7 * 24 * time.Hour
	col := makeTestCollection(t, app, "gc_empty")
	if err := app.Delete(col); err != nil {
		t.Fatal(err)
	}
	cursor, err := maxRowID(app.DB())
	if err != nil {
		t.Fatal(err)
	}
	ageGCState(t, app)
	if _, err := app.NonconcurrentDB().NewQuery(`CREATE TRIGGER gc_fail BEFORE DELETE ON _repl_oplog
		WHEN OLD.op_type = 'col_delete' BEGIN SELECT RAISE(ABORT, 'GC rollback'); END`).Execute(); err != nil {
		t.Fatal(err)
	}
	s, err := r.Compact(context.Background())
	if err == nil || !strings.Contains(err.Error(), "GC rollback") || s.Batches != 0 {
		t.Fatalf("expected rolled-back batch: %+v %v", s, err)
	}
	var horizons int
	if err := app.DB().NewQuery("SELECT COUNT(*) FROM _repl_compaction").Row(&horizons); err != nil || horizons != 0 {
		t.Fatalf("rollback leaked horizon: %d %v", horizons, err)
	}
	if progress, _ := getState(app.DB(), stateGCPass); progress != "" {
		t.Fatal("rollback advanced resume cursor")
	}
	if head, _ := maxRowID(app.DB()); head != cursor {
		t.Fatal("rollback removed highest operation")
	}
	if _, err := app.NonconcurrentDB().NewQuery("DROP TRIGGER gc_fail").Execute(); err != nil {
		t.Fatal(err)
	}
	completeGC(t, r)
	if head, _ := maxRowID(app.DB()); head != 0 {
		t.Fatalf("oplog not empty: %d", head)
	}
	// Reinitialization must preserve the allocation high-water mark too.
	if err := createTables(app); err != nil {
		t.Fatal(err)
	}
	liveCol := makeTestCollection(t, app, "gc_after_empty")
	ops, head, err := opsAfterRowID(app.DB(), cursor, 10)
	if err != nil || len(ops) != 1 || head <= cursor {
		t.Fatalf("push cursor skipped new write after empty GC: %d %d %v", len(ops), head, err)
	}
	transient := makeTestCollection(t, app, "gc_highest")
	if err := app.Delete(transient); err != nil {
		t.Fatal(err)
	}
	highest, _ := maxRowID(app.DB())
	ageGCState(t, app)
	completeGC(t, r)
	if head, _ := maxRowID(app.DB()); head == 0 || head >= highest {
		t.Fatalf("expected GC to remove highest rows but retain live state: %d / %d", head, highest)
	}
	saveTestRecord(t, app, liveCol.Name, "after highest deletion")
	if ops, head, err := opsAfterRowID(app.DB(), highest, 10); err != nil || len(ops) != 1 || head <= highest {
		t.Fatalf("push cursor skipped new write after highest deletion: %d %d %v", len(ops), head, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := r.Compact(ctx); !errors.Is(err, context.Canceled) || s.Batches != 0 {
		t.Fatalf("cancellation ignored: %+v %v", s, err)
	}
}

func TestRetentionNormalDeleteCaptureRollsBackAtomically(t *testing.T) {
	app := newTestAppOnly(t)
	if err := app.ResetBootstrapState(); err != nil {
		t.Fatal(err)
	}
	r, err := Register(app, Config{NodeID: "nodeA0000000001", ClusterSecret: testSecret})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err := app.RunAllMigrations(); err != nil {
		t.Fatal(err)
	}
	if !r.ready.Load() || r.replLn != nil {
		t.Fatal("bootstrap must enable capture without starting replication listeners")
	}
	col := makeTestCollection(t, app, "retention_capture")
	rec := saveTestRecord(t, app, col.Name, "live")
	before, _ := getVersion(app.DB(), col.Id, rec.Id)
	seq, _ := getState(app.DB(), stateLocalSeq)
	head, _ := maxRowID(app.DB())
	rollback := errors.New("retention rollback")
	if err := app.RunInTransaction(func(txApp core.App) error {
		if err := txApp.DeleteWithContext(context.Background(), rec); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if _, err := app.FindRecordById(col, rec.Id); err != nil {
		t.Fatalf("rollback lost record: %v", err)
	}
	if got, _ := getVersion(app.DB(), col.Id, rec.Id); got == nil || *got != *before {
		t.Fatalf("rollback changed LWW state: %+v", got)
	}
	if got, _ := getState(app.DB(), stateLocalSeq); got != seq {
		t.Fatalf("rollback consumed source sequence: %s -> %s", seq, got)
	}
	if got, _ := maxRowID(app.DB()); got != head {
		t.Fatal("rollback leaked captured tombstone")
	}
	rec, _ = app.FindRecordById(col, rec.Id)
	if err := app.RunInTransaction(func(txApp core.App) error {
		return txApp.DeleteWithContext(context.Background(), rec)
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := getVersion(app.DB(), col.Id, rec.Id); got == nil || !got.Deleted {
		t.Fatal("normal host retention delete did not capture a tombstone")
	}
	if got := lastOps(t, r, 1)[0]; got.Type != opDelete || got.SrcNode != r.nodeID || got.SrcSeq <= 0 {
		t.Fatalf("missing captured delete operation: %+v", got)
	}
}

func TestCompactionAdmittedOperationsSurviveNewFence(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_pending")
	o := &op{
		SrcNode: "remote", SrcSeq: 1, HLC: r.clock.Now(),
		Type: opUpsert, ColID: col.Id, ColName: col.Name,
		RecordID: "pending00000001", Payload: []byte(`{"title":"queued before GC"}`),
	}
	if inserted, err := insertOpIfAbsent(app.NonconcurrentDB(), o); err != nil || !inserted {
		t.Fatalf("initial operation not admitted: %v %v", inserted, err)
	}
	if err := setState(app.NonconcurrentDB(), stateGCHLC, r.clock.Now()); err != nil {
		t.Fatal(err)
	}
	r.applyRecordBatch([]*op{o})
	if _, err := app.FindRecordById(col, o.RecordID); err != nil {
		t.Fatalf("GC fenced off an already-admitted operation: %v", err)
	}
	later := *o
	later.RecordID, later.SrcSeq = "notseen00000001", 2
	if inserted, err := insertOpIfAbsent(app.NonconcurrentDB(), &later); err != nil || inserted {
		t.Fatalf("fenced operation admitted after GC: %v %v", inserted, err)
	}
	r.compactMu.Lock()
	if _, err := r.Compact(context.Background()); err == nil {
		t.Fatal("concurrent GC was accepted")
	}
	r.compactMu.Unlock()
	r.cfg.CompactionTimeBudget = time.Nanosecond
	if s, err := r.Compact(context.Background()); err != nil || s.Batches != 0 {
		t.Fatalf("exhausted run budget ignored: %+v %v", s, err)
	}
}

func TestCompactionClientCapsAndMemberHorizon(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.CompactionBatchSize = 3
	db := app.NonconcurrentDB()
	if err := gcHorizon(db, "_repl_compaction", "gone", 99); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{
		`INSERT INTO _repl_members (node_id, removed, last_seen) VALUES ('gone', 1, '2000-01-01T00:00:00Z')`,
		`INSERT INTO _repl_client_ips (ip, last_seen) VALUES ('fresh', '2999-01-01T00:00:00Z'), ('old', '2000-01-01T00:00:00Z')`,
	} {
		if _, err := db.NewQuery(q).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < maxPathsPerIP+5; i++ {
		if _, err := db.NewQuery(`INSERT INTO _repl_client_paths (ip, path, count) VALUES ('fresh', {:path}, 1), ('old', {:path}, 1)`).
			Bind(dbx.Params{"path": fmt.Sprintf("/%03d", i)}).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	completeGC(t, r)
	var count int
	if err := db.NewQuery("SELECT COUNT(*) FROM _repl_client_paths").Row(&count); err != nil || count != maxPathsPerIP {
		t.Fatalf("path cap/orphan cleanup failed: %d %v", count, err)
	}
	if _, snap, err := opsAfterVector(app.DB(), map[string]int64{"gone": 1}, 100); err != nil || !snap {
		t.Fatalf("member purge lost tombstone horizon: %v %v", snap, err)
	}
}

func TestCompactionOrphanRepairWaitsForPendingLWWWinner(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_pending_winner")
	rec := saveTestRecord(t, app, col.Name, "raw-deleted")
	pending := *lastOps(t, r, 1)[0]
	pending.SrcNode, pending.SrcSeq = "zzzz-remote", 1
	if _, err := app.NonconcurrentDB().NewQuery("DELETE FROM {{" + col.Name + "}} WHERE id = {:id}").
		Bind(dbx.Params{"id": rec.Id}).Execute(); err != nil {
		t.Fatal(err)
	}
	if inserted, err := insertOpIfAbsent(app.NonconcurrentDB(), &pending); err != nil || !inserted {
		t.Fatalf("pending operation not inserted: %v %v", inserted, err)
	}
	ageGCState(t, app)
	if s := completeGC(t, r); s.Repaired != 0 {
		t.Fatalf("orphan repair overtook an admitted same-HLC LWW winner: %+v", s)
	}
	if err := r.applyOp(&pending); err != nil {
		t.Fatal(err)
	}
	if v, _ := getVersion(app.DB(), col.Id, rec.Id); v == nil || v.Deleted || v.SrcNode != pending.SrcNode {
		t.Fatalf("pending LWW winner was lost: %+v", v)
	}
}

// The exact production GC queries, with bound parameters, must stay on
// indexes even when SQLite statistics are stale (collected while the oplog
// was tiny) - the planner otherwise falls back to a full scan per candidate.
func TestCompactionCandidateChecksUseKeysetIndexes(t *testing.T) {
	app, _ := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "gc_plans")
	saveTestRecord(t, app, col.Name, "tiny")
	if _, err := app.NonconcurrentDB().NewQuery("ANALYZE").Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.NonconcurrentDB().NewQuery(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 5000)
		INSERT INTO _repl_oplog (src_node, src_seq, hlc, op_type, col_id, col_name, record_id, payload, files, created)
		SELECT 'bulk', i, printf('%016x', i), 'upsert', 'c', 'c', 'r'||i, '', '', '2999' FROM n`).Execute(); err != nil {
		t.Fatal(err)
	}
	// The collector's phase start repairs the stale statistics on its own
	// (non-concurrent) connection, which runs every GC query.
	if err := refreshStaleStats(app.NonconcurrentDB(), "_repl_oplog"); err != nil {
		t.Fatal(err)
	}
	rec := dbx.Params{"col": col.Id, "rec": "r", "hlc": "h", "src": "s", "type": opDelete,
		"id": 1, "cut": "2000-01-01T00:00:00Z", "cap": 9}
	queries := map[string]string{
		"keyset":                `SELECT rowid FROM _repl_oplog WHERE rowid > {:id} AND rowid <= {:cap} ORDER BY rowid LIMIT 2`,
		"newer record op":       newerOpSQL("('upsert','delete')"),
		"newer collection op":   newerOpSQL("('col_upsert','col_delete')"),
		"record ops remain":     opsRemainSQL("('upsert','delete')"),
		"collection ops remain": opsRemainSQL("('col_upsert','col_delete')"),
		"retained delete":       retainedDeleteSQL,
		"admitted (gcRejects)":  `SELECT EXISTS(SELECT 1 FROM _repl_oplog WHERE src_node = {:src} AND src_seq = {:id})`,
		"source catch-up": `SELECT rowid, * FROM _repl_oplog
			WHERE src_node = {:src} AND src_seq > {:id} ORDER BY src_seq LIMIT {:cap}`,
		"client ip cap":   clientsGCSQL,
		"client path cap": pathsGCSQL,
	}
	for name, q := range queries {
		var plan []struct {
			Detail string `db:"detail"`
		}
		if err := app.NonconcurrentDB().NewQuery("EXPLAIN QUERY PLAN " + q).Bind(rec).All(&plan); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		search := false
		for _, row := range plan {
			search = search || strings.Contains(row.Detail, "SEARCH")
			if (strings.Contains(row.Detail, "SCAN ") && !strings.Contains(row.Detail, "SCAN CONSTANT ROW")) ||
				strings.Contains(row.Detail, "TEMP B-TREE") {
				t.Fatalf("GC check %q is not bounded/indexed: %s", name, row.Detail)
			}
		}
		if !search {
			t.Fatalf("GC check %q has no indexed search: %+v", name, plan)
		}
	}
}

// The pusher reads the oplog by rowid range on the concurrent pool. Under
// statistics collected while the oplog was tiny SQLite plans that read as a
// full scan; the pusher's throttled check must restore the rowid seek on
// every pooled connection.
func TestPushReadsUseRowIDSeekAfterStaleStats(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "push_plans")
	saveTestRecord(t, app, col.Name, "tiny")
	if _, err := app.NonconcurrentDB().NewQuery("ANALYZE").Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.NonconcurrentDB().NewQuery(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 5000)
		INSERT INTO _repl_oplog (src_node, src_seq, hlc, op_type, col_id, col_name, record_id, payload, files, created)
		SELECT 'bulk', i, printf('%016x', i), 'upsert', 'c', 'c', 'r'||i, '', '', '2999' FROM n`).Execute(); err != nil {
		t.Fatal(err)
	}
	pushPlan := func() string {
		var plan []struct {
			Detail string `db:"detail"`
		}
		if err := app.DB().NewQuery("EXPLAIN QUERY PLAN SELECT rowid, * FROM _repl_oplog WHERE rowid > {:c} ORDER BY rowid LIMIT {:l}").
			Bind(dbx.Params{"c": 4000, "l": 10}).All(&plan); err != nil {
			t.Fatal(err)
		}
		details := make([]string, 0, len(plan))
		for _, row := range plan {
			details = append(details, row.Detail)
		}
		return strings.Join(details, "; ")
	}
	if plan := pushPlan(); !strings.Contains(plan, "SCAN _repl_oplog") {
		t.Skipf("SQLite no longer mis-plans under stale statistics (%s); guard untestable", plan)
	}
	r.statsCheckAt.Store(0)
	r.refreshOplogStats()
	for i := 0; i < 8; i++ { // several pooled connections
		if plan := pushPlan(); !strings.Contains(plan, "SEARCH _repl_oplog USING INTEGER PRIMARY KEY") {
			t.Fatalf("push read still not a rowid seek after refresh: %s", plan)
		}
	}
	// throttled: a second call within the interval does no work
	before := r.statsCheckAt.Load()
	r.refreshOplogStats()
	if r.statsCheckAt.Load() != before {
		t.Fatal("refreshOplogStats ignored its throttle")
	}
	ops, last, err := opsAfterRowID(app.DB(), 0, 3)
	if err != nil || len(ops) != 3 || last <= 0 {
		t.Fatalf("push read after refresh: %d ops, last %d, err %v", len(ops), last, err)
	}
}

// The stale-statistics check runs inside GC transactions and on the push
// path, so its rowid span must be two indexed endpoint lookups. A combined
// MAX(rowid) - MIN(rowid) aggregate is planned as a full table/index scan.
func TestStatsSpanUsesIndexedEndpointLookups(t *testing.T) {
	app, _ := newTestNode(t, "nodeA0000000001")
	for _, table := range gcPhaseTables {
		var plan []struct {
			Detail string `db:"detail"`
		}
		if err := app.NonconcurrentDB().NewQuery("EXPLAIN QUERY PLAN " + rowidSpanSQL(table)).All(&plan); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		searches := 0
		for _, row := range plan {
			if strings.Contains(row.Detail, "SEARCH "+table) {
				searches++
			}
			if strings.Contains(row.Detail, "SCAN ") && !strings.Contains(row.Detail, "SCAN CONSTANT ROW") {
				t.Fatalf("%s rowid span is not bounded: %s", table, row.Detail)
			}
		}
		if searches != 2 {
			t.Fatalf("%s rowid span: want 2 endpoint searches, got plan %+v", table, plan)
		}
	}
}

func TestStatsSpanEmptyAndSparseRowIDs(t *testing.T) {
	app, _ := newTestNode(t, "nodeA0000000001")
	db := app.NonconcurrentDB()
	if _, err := db.NewQuery("CREATE TABLE _zz_span (x TEXT)").Execute(); err != nil {
		t.Fatal(err)
	}
	span := func() int64 {
		t.Helper()
		n, err := rowidSpan(db, "_zz_span")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := span(); n != 0 {
		t.Fatalf("empty table span = %d, want 0", n)
	}
	if _, err := db.NewQuery("INSERT INTO _zz_span (rowid, x) VALUES (7, 'a')").Execute(); err != nil {
		t.Fatal(err)
	}
	if n := span(); n != 1 {
		t.Fatalf("single-row span = %d, want 1", n)
	}
	if _, err := db.NewQuery("INSERT INTO _zz_span (rowid, x) VALUES (100, 'b'), (5000, 'c')").Execute(); err != nil {
		t.Fatal(err)
	}
	if n := span(); n != 4994 {
		t.Fatalf("sparse span = %d, want 4994 (rowids 7..5000)", n)
	}
	if _, err := db.NewQuery("DELETE FROM _zz_span").Execute(); err != nil {
		t.Fatal(err)
	}
	if n := span(); n != 0 {
		t.Fatalf("emptied table span = %d, want 0", n)
	}
}
