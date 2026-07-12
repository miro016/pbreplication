package pbreplication

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestExportedStatusMirrorsState(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")

	makeTestCollection(t, app, "posts")
	r.stats.applied.Add(3)
	r.stats.failed.Add(1)
	r.notePeerVector("nodeB0000000001", map[string]int64{r.nodeID: 0})

	st := r.Status()
	if st.NodeID != r.nodeID {
		t.Fatalf("NodeID = %q", st.NodeID)
	}
	if st.Counters.Applied != 3 || st.Counters.Failed != 1 {
		t.Fatalf("counters wrong: %+v", st.Counters)
	}
	if st.Counters.OplogSize == 0 {
		t.Fatal("oplog size must reflect the captured collection op")
	}
	if st.Sync.Phase != SyncIdle {
		t.Fatalf("idle node sync phase = %q", st.Sync.Phase)
	}
	if _, ok := st.PeerLags["nodeB0000000001"]; !ok {
		t.Fatalf("peer lags missing entry: %+v", st.PeerLags)
	}
	if len(st.Members) == 0 || !st.Members[0].Self {
		t.Fatalf("members must include self: %+v", st.Members)
	}
	if st.HLC == "" {
		t.Fatal("HLC empty")
	}
}

func TestSyncStatusPublishAndClear(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	prog := &syncProgress{
		total: 200, done: 100,
		start: time.Now().Add(-10 * time.Second),
		phase: SyncResyncing,
		peer:  "nodeB0000000001",
	}
	r.publishSnapshotProgress(prog, "posts")

	st := r.SyncStatus()
	if st.Phase != SyncResyncing || st.Collection != "posts" || st.Peer != "nodeB0000000001" {
		t.Fatalf("published status wrong: %+v", st)
	}
	if st.Percent != 50 || st.DoneRows != 100 || st.TotalRows != 200 {
		t.Fatalf("progress numbers wrong: %+v", st)
	}
	if st.ETAString == "" {
		t.Fatalf("ETA missing: %+v", st)
	}

	// the JSON shape feeds the dashboard: phase + eta must serialize
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"phase":"resyncing"`) || !strings.Contains(string(b), `"eta"`) {
		t.Fatalf("unexpected JSON: %s", b)
	}

	r.clearProgress()
	if got := r.SyncStatus().Phase; got != SyncIdle {
		t.Fatalf("after clear phase = %q", got)
	}
}
