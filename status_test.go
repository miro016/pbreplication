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
	r.consoleIsTTY = true

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

func TestSubscribeSyncStatusCoalescesAndCloses(t *testing.T) {
	r := &Replicator{progressSubs: map[chan SyncStatus]struct{}{}, consoleIsTTY: true}
	updates, cancel := r.SubscribeSyncStatus(0)

	r.publishProgress(SyncStatus{Phase: SyncCopying, Percent: 10, BytesTotal: 100})
	r.publishProgress(SyncStatus{Phase: SyncCopying, Percent: 20, BytesTotal: 100})
	if got := <-updates; got.Phase != SyncCopying || got.Percent != 20 {
		t.Fatalf("subscriber received stale status: %+v", got)
	}

	cancel()
	if _, open := <-updates; open {
		t.Fatal("subscription channel remained open after cancel")
	}
	// Cancellation is idempotent.
	cancel()
}

func TestServiceSyncProgressFormatting(t *testing.T) {
	status := SyncStatus{
		Phase:      SyncCopying,
		Peer:       "nodeB0000000001",
		Percent:    50,
		BytesDone:  32 << 20,
		BytesTotal: 64 << 20,
		ETAString:  "12s",
	}
	got := formatServiceSyncProgress(status)
	want := "phase=copying peer=nodeB0000000001 progress=50% bytes=33554432/67108864 eta=12s"
	if got != want {
		t.Fatalf("formatServiceSyncProgress() = %q, want %q", got, want)
	}
	if bucket := syncStatusProgressBucket(status); bucket != 5 {
		t.Fatalf("syncStatusProgressBucket() = %d, want 5", bucket)
	}
}

func TestServiceSyncProgressThrottle(t *testing.T) {
	r := &Replicator{}
	started := time.Unix(1_800_000_000, 0)
	status := SyncStatus{Phase: SyncCopying, Percent: 1, BytesDone: 1, BytesTotal: 100}

	r.logServiceSyncProgress(status, started)
	if !r.progressLog.lastLog.Equal(started) {
		t.Fatal("first service progress update was not logged")
	}

	r.logServiceSyncProgress(status, started.Add(time.Second))
	if !r.progressLog.lastLog.Equal(started) {
		t.Fatal("same progress bucket bypassed the service log throttle")
	}

	status.Percent = 10
	status.BytesDone = 10
	nextBucket := started.Add(2 * time.Second)
	r.logServiceSyncProgress(status, nextBucket)
	if !r.progressLog.lastLog.Equal(nextBucket) {
		t.Fatal("10 percent boundary was not logged")
	}

	heartbeat := nextBucket.Add(serviceProgressLogInterval)
	r.logServiceSyncProgress(status, heartbeat)
	if !r.progressLog.lastLog.Equal(heartbeat) {
		t.Fatal("service progress heartbeat was not logged")
	}
}
