package pbreplication

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

func snapshotOp(col *core.Collection, srcNode, hlcStr, id, title string) *op {
	payload, _ := json.Marshal(map[string]any{"id": id, "title": title})
	return &op{
		SrcNode:  srcNode,
		HLC:      hlcStr,
		Type:     opUpsert,
		ColID:    col.Id,
		ColName:  col.Name,
		RecordID: id,
		Payload:  payload,
	}
}

func TestApplySnapshotBatchLWW(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "posts")

	// a fresh local write establishes a NEWER version for rec2
	rec := core.NewRecord(col)
	rec.Id = "rec200000000002"
	rec.Set("title", "local-newer")
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}

	oldHLC := encodeHLC(1000, 0) // far in the past -> loses against the live write
	newHLC := r.clock.Now()

	ops := []*op{
		snapshotOp(col, "nodeB0000000001", newHLC, "rec100000000001", "remote-1"),
		snapshotOp(col, "nodeB0000000001", oldHLC, "rec200000000002", "remote-stale"),
		snapshotOp(col, "nodeB0000000001", newHLC, "rec300000000003", "remote-3"),
	}

	applied, err := r.applySnapshotBatch(col, ops)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2 (stale op must be LWW-gated)", applied)
	}

	got, err := app.FindRecordById(col, "rec200000000002")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetString("title") != "local-newer" {
		t.Fatalf("stale snapshot op overwrote newer local write: %q", got.GetString("title"))
	}
	if _, err := app.FindRecordById(col, "rec100000000001"); err != nil {
		t.Fatal("batch record rec1 missing")
	}
	if _, err := app.FindRecordById(col, "rec300000000003"); err != nil {
		t.Fatal("batch record rec3 missing")
	}

	// re-applying the same batch is a no-op (idempotent resume replay)
	applied, err = r.applySnapshotBatch(col, ops)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("replay applied = %d, want 0", applied)
	}
}

func TestSnapshotResumePersistAndReuse(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	run := r.loadOrStartSnapshotResume("seed000000000001", true)
	if run.RunID == "" || len(run.Cols) != 0 {
		t.Fatalf("fresh run malformed: %+v", run)
	}
	run.Cols["posts"] = "cursor123"
	run.Cols["users"] = snapshotColDone
	r.saveSnapshotResume(run)

	// same peer + same mode -> resumed with cursors intact
	again := r.loadOrStartSnapshotResume("seed000000000001", true)
	if again.RunID != run.RunID {
		t.Fatalf("expected resume of run %s, got %s", run.RunID, again.RunID)
	}
	if again.Cols["posts"] != "cursor123" || again.Cols["users"] != snapshotColDone {
		t.Fatalf("cursors lost: %+v", again.Cols)
	}

	// different mode -> fresh run
	fresh := r.loadOrStartSnapshotResume("seed000000000001", false)
	if fresh.RunID == run.RunID {
		t.Fatal("run must not be resumed across reconcile-mode changes")
	}

	// clearing removes the state
	r.clearSnapshotResume(fresh)
	if raw, _ := getState(r.app.DB(), stateSnapshotResume); raw != "" {
		t.Fatalf("resume state not cleared: %q", raw)
	}
}

func TestReconcilePagedDeletesOnlyPreHorizon(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "posts")

	// three local records
	for _, id := range []string{"aaa000000000001", "bbb000000000002", "ccc000000000003"} {
		rec := core.NewRecord(col)
		rec.Id = id
		rec.Set("title", id)
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
	}

	// pretend records aaa and bbb were written long before the horizon
	preHorizon := encodeHLC(1000, 0)
	for _, id := range []string{"aaa000000000001", "bbb000000000002"} {
		if err := upsertVersion(app.NonconcurrentDB(), col.Id, id, preHorizon, r.nodeID, false); err != nil {
			t.Fatal(err)
		}
	}

	// the peer only has bbb (seen-set contains bbb); ccc is a recent
	// local write (its capture HLC is far above the horizon)
	runID := "testrun001"
	if err := insertSyncSeen(app.NonconcurrentDB(), runID, col.Id, []string{"bbb000000000002"}); err != nil {
		t.Fatal(err)
	}

	horizon := encodeHLC(2000, 0)
	r.reconcileCollection(col, runID, horizon)

	if _, err := app.FindRecordById(col, "aaa000000000001"); err == nil {
		t.Fatal("aaa should have been reconcile-deleted (absent on peer, pre-horizon)")
	}
	if _, err := app.FindRecordById(col, "bbb000000000002"); err != nil {
		t.Fatal("bbb was seen on the peer and must survive")
	}
	if _, err := app.FindRecordById(col, "ccc000000000003"); err != nil {
		t.Fatal("ccc is a post-horizon local write and must survive")
	}
}

func TestAdvanceVectorBatched(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	db := app.NonconcurrentDB()

	// contiguous run longer than one internal batch (1024), then a hole
	const n = 2500
	for i := 1; i <= n; i++ {
		if err := insertOp(db, &op{
			SrcNode: "nodeB0000000001", SrcSeq: int64(i), HLC: fmt.Sprintf("%016x-0000", i),
			Type: opUpsert, ColID: "c", ColName: "c", RecordID: fmt.Sprintf("r%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	// hole at n+1; n+2 present
	if err := insertOp(db, &op{
		SrcNode: "nodeB0000000001", SrcSeq: int64(n + 2), HLC: "ffff000000000000-0000",
		Type: opUpsert, ColID: "c", ColName: "c", RecordID: "rX",
	}); err != nil {
		t.Fatal(err)
	}

	next, err := advanceVector(db, "nodeB0000000001", 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != n {
		t.Fatalf("advanceVector = %d, want %d (stop at the hole)", next, n)
	}

	var persisted string
	if err := db.NewQuery(`SELECT value FROM _repl_state WHERE key = {:k}`).
		Bind(dbx.Params{"k": stateVectorPrefix + "nodeB0000000001"}).Row(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != fmt.Sprintf("%d", n) {
		t.Fatalf("persisted vector = %s, want %d", persisted, n)
	}
	_ = r
}
