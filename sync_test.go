package pbreplication

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
)

// ---------------------------------------------------------------------
// notePeerErr / clearPeerErr

func TestClearPeerErrRetractsGlobalLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	r.notePeerErr("nodeA0000000001", errors.New("dial tcp: connection refused"))

	if v, _ := r.memberErrs.Load("nodeA0000000001"); v != "dial tcp: connection refused" {
		t.Fatalf("member error not recorded: %v", v)
	}
	if le := r.LastError(); !strings.Contains(le, "sync with peer nodeA0000000001 failing") {
		t.Fatalf("global last error not recorded: %q", le)
	}

	// Peer recovers -> both the per-member error and the global banner
	// referring to it must disappear.
	r.clearPeerErr("nodeA0000000001")

	if v, _ := r.memberErrs.Load("nodeA0000000001"); v != "" {
		t.Fatalf("member error not cleared: %v", v)
	}
	if le := r.LastError(); le != "" {
		t.Fatalf("global last error should be cleared after rejoin, got %q", le)
	}
}

func TestClearPeerErrKeepsUnrelatedLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	// An unrelated error recorded after the peer failure must survive
	// the peer's recovery.
	r.notePeerErr("nodeA0000000001", errors.New("connection refused"))
	r.logError("compaction failed", errors.New("disk full"))

	r.clearPeerErr("nodeA0000000001")

	if le := r.LastError(); !strings.Contains(le, "compaction failed") {
		t.Fatalf("unrelated last error was lost: %q", le)
	}
}

func TestClearPeerErrKeepsOtherPeersLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	r.notePeerErr("nodeA0000000001", errors.New("connection refused"))
	r.notePeerErr("nodeB0000000001", errors.New("connection refused"))

	// nodeB's failure is the most recent global error; nodeA recovering
	// must not clear it.
	r.clearPeerErr("nodeA0000000001")

	if le := r.LastError(); !strings.Contains(le, "nodeB0000000001") {
		t.Fatalf("other peer's last error was lost: %q", le)
	}
}

func TestPullFromPeerPaginatesAcrossCompactionGap(t *testing.T) {
	seedApp, _ := newTestNode(t, "seed000000000001")
	_, receiver := newTestNode(t, "receiver00000001")
	receiver.cfg.MaxBatch = 3

	const source = "source0000000001"
	if err := setState(receiver.app.DB(), stateVectorPrefix+source, "1"); err != nil {
		t.Fatal(err)
	}
	// Sequence 2 represents an operation removed as superseded by compaction.
	// More than one full page remains after the gap, reproducing the production
	// loop where the persisted contiguous vector cannot advance past the hole.
	for seq := int64(3); seq <= 8; seq++ {
		o := &op{
			SrcNode: source, SrcSeq: seq, HLC: fmt.Sprintf("%016x-0000", seq),
			Type: opUpsert, ColID: "collection", ColName: "records",
			RecordID: fmt.Sprintf("record-%d", seq), Payload: json.RawMessage(`{"value":true}`),
		}
		if err := insertOp(seedApp.DB(), o); err != nil {
			t.Fatal(err)
		}
		// The receiver can already hold operations beyond its contiguous
		// vector (as in the production failure); replay must not enqueue them.
		if err := insertOp(receiver.app.DB(), o); err != nil {
			t.Fatal(err)
		}
	}

	var requestedAfter []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var req pullRequest
		if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requestedAfter = append(requestedAfter, req.Vector[source])
		ops, snapshotRequired, err := opsAfterVector(seedApp.DB(), req.Vector, req.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&pullResponse{
			NodeID: "seed000000000001", Ops: ops,
			Vector: map[string]int64{source: 8}, SnapshotRequired: snapshotRequired,
		})
	}))
	t.Cleanup(server.Close)

	if err := receiver.pullFromPeer(&member{NodeID: "seed000000000001", URL: server.URL}); err != nil {
		t.Fatal(err)
	}
	if want := []int64{1, 5, 8}; !reflect.DeepEqual(requestedAfter, want) {
		t.Fatalf("pull cursors = %v, want %v", requestedAfter, want)
	}
	if got, err := loadVectorEntry(receiver.app.DB(), source); err != nil || got != 8 {
		t.Fatalf("adopted vector = %d, %v; want 8", got, err)
	}
	var retained int
	if err := receiver.app.DB().NewQuery(`SELECT COUNT(*) FROM _repl_oplog WHERE src_node = {:s}`).
		Bind(dbx.Params{"s": source}).Row(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 6 {
		t.Fatalf("retained ops = %d, want 6", retained)
	}
	if queued := len(receiver.applyCh); queued != 0 {
		t.Fatalf("duplicate pull queued %d already-retained ops, want 0", queued)
	}
}

func TestIngestOpsDoesNotEnqueueDuplicates(t *testing.T) {
	_, receiver := newTestNode(t, "receiver00000001")
	o := &op{
		SrcNode: "source0000000001", SrcSeq: 1, HLC: "0000000000000001-0000",
		Type: opUpsert, ColID: "collection", ColName: "records", RecordID: "record-1",
		Payload: json.RawMessage(`{"value":true}`),
	}

	if err := receiver.ingestOps([]*op{o}); err != nil {
		t.Fatal(err)
	}
	if err := receiver.ingestOps([]*op{o}); err != nil {
		t.Fatal(err)
	}
	if got := len(receiver.applyCh); got != 1 {
		t.Fatalf("apply queue length = %d, want 1 for one unique operation", got)
	}
}

func TestIngestOpsCountExcludesDuplicatesAndEchoedLocalOps(t *testing.T) {
	_, receiver := newTestNode(t, "receiver00000001")
	remote := &op{
		SrcNode: "source0000000001", SrcSeq: 1, HLC: "0000000000000001-0000",
		Type: opUpsert, ColID: "collection", ColName: "records", RecordID: "remote",
		Payload: json.RawMessage(`{"value":true}`),
	}
	localEcho := &op{
		SrcNode: receiver.nodeID, SrcSeq: 1, HLC: "0000000000000002-0000",
		Type: opUpsert, ColID: "collection", ColName: "records", RecordID: "local",
		Payload: json.RawMessage(`{"value":true}`),
	}

	if got, err := receiver.ingestOpsCount([]*op{remote, localEcho}); err != nil || got != 1 {
		t.Fatalf("first ingest count = %d, %v; want 1", got, err)
	}
	if got, err := receiver.ingestOpsCount([]*op{remote, localEcho}); err != nil || got != 0 {
		t.Fatalf("repeat ingest count = %d, %v; want 0", got, err)
	}
}

func TestSuccessfulPullRefreshesMemberSnapshotUsedForHealth(t *testing.T) {
	app, receiver := newTestNode(t, "receiver00000001")
	peer := &member{
		NodeID: "seed000000000001",
		URL:    "",
		// Deliberately stale enough to be unhealthy before the exchange.
		LastSeen: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&pullResponse{
			NodeID: peer.NodeID,
			Vector: map[string]int64{},
		})
	}))
	t.Cleanup(server.Close)
	peer.URL = server.URL
	if err := upsertMember(app.DB(), peer); err != nil {
		t.Fatal(err)
	}

	if err := receiver.pullFromPeer(peer); err != nil {
		t.Fatal(err)
	}
	if !receiver.isHealthy(peer) {
		t.Fatalf("successful pull retained stale last_seen %q", peer.LastSeen)
	}
}

func TestSkipRequesterOwnedOpsPreventsEcho(t *testing.T) {
	request := map[string]int64{
		"requester000001": 10,
		"other0000000001": 4,
	}
	peer := map[string]int64{
		"requester000001": 25,
		"other0000000001": 8,
	}

	got := skipRequesterOwnedOps(request, "requester000001", peer)
	if got["requester000001"] != 25 {
		t.Fatalf("requester vector = %d, want 25", got["requester000001"])
	}
	if got["other0000000001"] != 4 {
		t.Fatalf("unrelated vector changed to %d", got["other0000000001"])
	}
}

func TestRunAfterBootstrapGatesSynchronization(t *testing.T) {
	r := &Replicator{stopCh: make(chan struct{})}
	ready := make(chan struct{})
	started := make(chan struct{})
	r.wg.Add(1)
	go r.runAfterBootstrap(ready, func() {
		close(started)
		r.wg.Done()
	})

	select {
	case <-started:
		t.Fatal("synchronization started before bootstrap completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(ready)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("synchronization did not start after bootstrap completed")
	}
	r.wg.Wait()
}
