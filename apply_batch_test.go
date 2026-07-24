package pbreplication

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

func saveTestRecord(t *testing.T, app core.App, colName, title string) *core.Record {
	t.Helper()
	col, err := app.FindCollectionByNameOrId(colName)
	if err != nil {
		t.Fatal(err)
	}
	rec := core.NewRecord(col)
	rec.Set("title", title)
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestApplyBatchAcrossCollections(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "posts")
	makeTestCollection(t, appA, "authors")
	for _, o := range lastOps(t, rA, 2) {
		if err := rB.applyOp(o); err != nil {
			t.Fatal(err)
		}
	}

	// six creates across two collections, plus an update to the first
	// record — FIFO within the batch must make the update win
	recs := make([]*core.Record, 0, 6)
	for i := 0; i < 6; i++ {
		colName := "posts"
		if i%2 == 1 {
			colName = "authors"
		}
		recs = append(recs, saveTestRecord(t, appA, colName, "v1"))
	}
	recs[0].Set("title", "v2")
	if err := appA.Save(recs[0]); err != nil {
		t.Fatal(err)
	}

	ops := lastOps(t, rA, 7)
	before := rB.stats.applied.Load()
	rB.applyBatch(ops)
	if got := rB.stats.applied.Load() - before; got != 7 {
		t.Fatalf("expected 7 applied ops, got %d", got)
	}

	for i, rec := range recs {
		got, err := appB.FindRecordById(rec.Collection().Name, rec.Id)
		if err != nil {
			t.Fatalf("record %d missing on B: %v", i, err)
		}
		want := "v1"
		if i == 0 {
			want = "v2"
		}
		if got.GetString("title") != want {
			t.Fatalf("record %d title = %q, want %q", i, got.GetString("title"), want)
		}
	}

	// version rows must match the last op per record
	lastHLC := map[string]string{}
	for _, o := range ops {
		lastHLC[o.ColID+"/"+o.RecordID] = o.HLC
	}
	for _, o := range ops {
		ver, err := getVersion(appB.DB(), o.ColID, o.RecordID)
		if err != nil || ver == nil {
			t.Fatalf("missing version row for %s: %v", o.RecordID, err)
		}
		if ver.HLC != lastHLC[o.ColID+"/"+o.RecordID] {
			t.Fatalf("version hlc = %q, want %q", ver.HLC, lastHLC[o.ColID+"/"+o.RecordID])
		}
	}
}

func TestApplyBatchMixedOpsAndStaleLWW(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "posts")
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}

	x := saveTestRecord(t, appA, "posts", "new")
	xOp := lastOps(t, rA, 1)[0]

	y := saveTestRecord(t, appA, "posts", "doomed")
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}
	if err := appA.Delete(y); err != nil {
		t.Fatal(err)
	}
	yDel := lastOps(t, rA, 1)[0]

	z := saveTestRecord(t, appA, "posts", "old")
	zStale := lastOps(t, rA, 1)[0]
	z.Set("title", "current")
	if err := appA.Save(z); err != nil {
		t.Fatal(err)
	}
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err) // B already has the NEWER version of Z
	}

	before := rB.stats.applied.Load()
	rB.applyBatch([]*op{xOp, yDel, zStale})
	if got := rB.stats.applied.Load() - before; got != 2 {
		t.Fatalf("expected 2 applied ops (stale one skipped), got %d", got)
	}

	if _, err := appB.FindRecordById("posts", x.Id); err != nil {
		t.Fatalf("upsert not applied: %v", err)
	}
	if _, err := appB.FindRecordById("posts", y.Id); err == nil {
		t.Fatal("delete not applied")
	}
	ver, _ := getVersion(appB.DB(), yDel.ColID, y.Id)
	if ver == nil || !ver.Deleted {
		t.Fatalf("expected tombstone version for deleted record, got %+v", ver)
	}
	got, err := appB.FindRecordById("posts", z.Id)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetString("title") != "current" {
		t.Fatalf("stale op overwrote newer record: title = %q", got.GetString("title"))
	}
}

func TestApplyBatchCollectionOpBarrier(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "posts")
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}

	// record op, then a schema op, then a record op that NEEDS the new
	// schema — all inside one batch
	p := saveTestRecord(t, appA, "posts", "before-schema")
	makeTestCollection(t, appA, "tags")
	tg := saveTestRecord(t, appA, "tags", "after-schema")

	rB.applyBatch(lastOps(t, rA, 3))

	if _, err := appB.FindRecordById("posts", p.Id); err != nil {
		t.Fatalf("record before schema op missing: %v", err)
	}
	if col, err := appB.FindCollectionByNameOrId("tags"); err != nil || col == nil {
		t.Fatalf("collection op not applied: %v", err)
	}
	if _, err := appB.FindRecordById("tags", tg.Id); err != nil {
		t.Fatalf("record after schema op missing (barrier violated): %v", err)
	}
	if got := rB.pendingCount(); got != 0 {
		t.Fatalf("no ops should be parked, got %d", got)
	}
}

func TestApplyBatchPoisonedOpFallback(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "posts")
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}

	g1 := saveTestRecord(t, appA, "posts", "good-1")
	g1Op := lastOps(t, rA, 1)[0]
	g2 := saveTestRecord(t, appA, "posts", "good-2")
	g2Op := lastOps(t, rA, 1)[0]

	poison := &op{
		SrcNode:  rA.NodeID(),
		SrcSeq:   999999,
		HLC:      encodeHLC(uint64(time.Now().UnixMilli()), 1),
		Type:     opUpsert,
		ColID:    g1Op.ColID,
		ColName:  g1Op.ColName,
		RecordID: "poisonrec123456",
		Payload:  json.RawMessage(`{"title":`), // malformed on purpose
	}

	beforeApplied := rB.stats.applied.Load()
	beforeFailed := rB.stats.failed.Load()
	rB.applyBatch([]*op{g1Op, poison, g2Op})

	if _, err := appB.FindRecordById("posts", g1.Id); err != nil {
		t.Fatalf("good op 1 sunk by poisoned neighbour: %v", err)
	}
	if _, err := appB.FindRecordById("posts", g2.Id); err != nil {
		t.Fatalf("good op 2 sunk by poisoned neighbour: %v", err)
	}
	if got := rB.stats.applied.Load() - beforeApplied; got != 2 {
		t.Fatalf("expected 2 applied ops via fallback, got %d", got)
	}
	if got := rB.stats.failed.Load() - beforeFailed; got != 1 {
		t.Fatalf("expected exactly 1 failed op, got %d", got)
	}

	found := false
	for _, ev := range rB.Events(50) {
		if ev.Type == EventOpFailed && ev.Fields["record"] == "poisonrec123456" {
			found = true
		}
	}
	if !found {
		t.Fatal("poisoned op failure not surfaced on the event timeline")
	}
}

func TestApplyBatchParkedOpAppliedAfterSchemaArrives(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "later")
	colOp := lastOps(t, rA, 1)[0]
	l := saveTestRecord(t, appA, "later", "parked")
	lOp := lastOps(t, rA, 1)[0]

	// record op arrives before its collection exists on B -> parked
	rB.applyBatch([]*op{lOp})
	if got := rB.pendingCount(); got != 1 {
		t.Fatalf("expected 1 parked op, got %d", got)
	}
	if _, err := appB.FindRecordById("later", l.Id); err == nil {
		t.Fatal("record applied without its collection")
	}

	// the schema op arrives -> parked op is re-enqueued...
	rB.applyBatch([]*op{colOp})
	if got := rB.pendingCount(); got != 0 {
		t.Fatalf("op still parked after its collection arrived: %d", got)
	}

	// ...and sits on the apply queue (the applier goroutine isn't
	// running in tests, so drain it by hand)
	select {
	case o := <-rB.applyCh:
		rB.applyBatch([]*op{o})
	default:
		t.Fatal("parked op was not re-enqueued")
	}
	if _, err := appB.FindRecordById("later", l.Id); err != nil {
		t.Fatalf("parked op lost: %v", err)
	}
}

func TestApplyQueueCapacityFollowsBatchSize(t *testing.T) {
	if got := applyQueueCap(200); got != 4096 {
		t.Fatalf("default batch: expected floor of 4096, got %d", got)
	}
	if got := applyQueueCap(2000); got != 8000 {
		t.Fatalf("large batch: expected 4x batch = 8000, got %d", got)
	}

	_, r := newTestNode(t, "nodeA0000000001")
	if got := cap(r.applyCh); got != 4096 {
		t.Fatalf("default apply queue capacity: expected 4096, got %d", got)
	}
	if r.cfg.ApplyBatch != 200 {
		t.Fatalf("default ApplyBatch: expected 200, got %d", r.cfg.ApplyBatch)
	}
}
