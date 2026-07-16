package pbreplication

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/router"
)

// ---------------------------------------------------------------------
// helpers

// execHandler invokes a route handler directly with a synthetic request.
func execHandler(t *testing.T, r *Replicator, h func(*core.RequestEvent) error, method, target, body string) (*httptest.ResponseRecorder, error) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	ev := &core.RequestEvent{App: r.app}
	ev.Request = req
	ev.Response = rec
	return rec, h(ev)
}

// serveReplicator exposes a replicator's REAL join/ping handlers over
// HTTP (cluster auth is a router middleware and not part of the
// handlers, so it is intentionally absent here).
func serveReplicator(t *testing.T, r *Replicator) *httptest.Server {
	t.Helper()
	wrap := func(h func(*core.RequestEvent) error) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			ev := &core.RequestEvent{App: r.app}
			ev.Request = req
			ev.Response = w
			if err := h(ev); err != nil {
				apiErr := router.ToApiError(err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(apiErr.Status)
				_ = json.NewEncoder(w).Encode(apiErr)
			}
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/ping", wrap(r.handlePing))
	mux.HandleFunc("/api/replication/join", wrap(r.handleJoin))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// cloneNode simulates "cp -r pb_data": a new node whose replication
// identity, oplog, membership and data are copies of src's.
func cloneNode(t *testing.T, src *Replicator, seedURL string) (*tests.TestApp, *Replicator) {
	t.Helper()
	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID:        src.NodeID(), // the cloned identity
		SeedURL:       seedURL,
		ClusterSecret: testSecret,
	})

	ops, _, err := opsAfterRowID(src.app.DB(), 0, 100000)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range ops {
		// materialize schema/records (applyOp skips own-node ops, so
		// apply under a throwaway source), then copy the oplog verbatim
		applied := *o
		applied.SrcNode = "cloneseed000001"
		if err := r.applyOp(&applied); err != nil {
			t.Fatal(err)
		}
		if err := insertOp(app.DB(), o); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := getState(src.app.DB(), stateLocalSeq); v != "" {
		if err := setState(app.DB(), stateLocalSeq, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := setState(app.DB(), stateBootstrapDone, nowStr()); err != nil {
		t.Fatal(err)
	}
	members, err := listMembers(src.app.DB(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if err := upsertMember(app.DB(), m); err != nil {
			t.Fatal(err)
		}
	}
	return app, r
}

func srcRows(t *testing.T, r *Replicator, srcNode string) []oplogRow {
	t.Helper()
	var rows []oplogRow
	if err := r.app.DB().NewQuery(`SELECT rowid, * FROM _repl_oplog WHERE src_node = {:s} ORDER BY src_seq`).
		Bind(dbx.Params{"s": srcNode}).All(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func hasEvent(r *Replicator, typ EventType) bool {
	for _, ev := range r.Events(0) {
		if ev.Type == typ {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------
// server side of the join handshake

func TestHandleJoinDuplicateHandling(t *testing.T) {
	joinBody := func(t *testing.T, req *joinRequest) string {
		t.Helper()
		b, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	t.Run("foreign process with our id is rejected with 409", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		_, err := execHandler(t, r, r.handleJoin, http.MethodPost, "/api/replication/join",
			joinBody(t, &joinRequest{NodeID: r.NodeID(), InstanceID: "someotherprocess"}))

		var apiErr *router.ApiError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
			t.Fatalf("err = %v, want 409 ApiError", err)
		}
		if !hasEvent(r, EventDuplicateNode) {
			t.Fatal("a duplicate_node_id event must be emitted")
		}
		members, _ := listMembers(app.DB(), true)
		if len(members) != 1 {
			t.Fatalf("members = %d, the duplicate must not be registered", len(members))
		}
	})

	t.Run("loopback with our own instance id is tolerated", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		rec, err := execHandler(t, r, r.handleJoin, http.MethodPost, "/api/replication/join",
			joinBody(t, &joinRequest{NodeID: r.NodeID(), InstanceID: r.instanceID}))
		if err != nil {
			t.Fatalf("self-join must succeed: %v", err)
		}
		var resp joinResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.NodeID != r.NodeID() || resp.InstanceID != r.instanceID {
			t.Fatalf("response must echo the node and instance id: %+v", resp)
		}
		members, _ := listMembers(app.DB(), true)
		if len(members) != 1 {
			t.Fatalf("members = %d, a self-join must not add rows", len(members))
		}
	})

	t.Run("old-version joiner without an instance id is tolerated", func(t *testing.T) {
		// also the shape fetchSeedAck sends on purpose - it must receive
		// the vector instead of a 409
		app, r := newTestNode(t, "nodeA0000000001")
		makeTestCollection(t, app, "posts")

		rec, err := execHandler(t, r, r.handleJoin, http.MethodPost, "/api/replication/join",
			joinBody(t, &joinRequest{NodeID: r.NodeID()}))
		if err != nil {
			t.Fatalf("instance-less join must succeed: %v", err)
		}
		var resp joinResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Vector[r.NodeID()] != 1 {
			t.Fatalf("vector = %v, want own seq 1", resp.Vector)
		}
	})

	t.Run("normal join with a different id still registers", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		_, err := execHandler(t, r, r.handleJoin, http.MethodPost, "/api/replication/join",
			joinBody(t, &joinRequest{NodeID: "othernode000001", InstanceID: "someotherprocess"}))
		if err != nil {
			t.Fatalf("regular join must succeed: %v", err)
		}
		if m, _ := getMember(app.DB(), "othernode000001"); m == nil {
			t.Fatal("joiner must be registered as a member")
		}
	})

	t.Run("invalid body is a 400", func(t *testing.T) {
		_, r := newTestNode(t, "nodeA0000000001")
		_, err := execHandler(t, r, r.handleJoin, http.MethodPost, "/api/replication/join", `{"node_id":""}`)
		var apiErr *router.ApiError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("err = %v, want 400 ApiError", err)
		}
	})
}

// ---------------------------------------------------------------------
// adoptFreshNodeID edge cases

func TestAdoptFreshNodeIDEdgeCases(t *testing.T) {
	t.Run("empty oplog", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		if err := r.adoptFreshNodeID(-1); err != nil {
			t.Fatal(err)
		}
		if r.NodeID() == oldID {
			t.Fatal("id must change even with no history")
		}
		if v, _ := getState(app.DB(), stateLocalSeq); v != "0" {
			t.Fatalf("local_seq = %q, want 0", v)
		}
		if seq, _ := loadVectorEntry(app.DB(), oldID); seq != 0 {
			t.Fatalf("vector[old] = %d, want 0", seq)
		}
	})

	t.Run("seed ack beyond local history is clamped", func(t *testing.T) {
		// the original owner wrote MORE after the clone was taken: the
		// vector entry must reflect what we actually hold (maxOwn), so
		// anti-entropy pulls the rest instead of skipping it
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		makeTestCollection(t, app, "posts") // 1 op

		if err := r.adoptFreshNodeID(100); err != nil {
			t.Fatal(err)
		}
		if got := countOps(t, r, oldID); got != 1 {
			t.Fatalf("old ops = %d, want 1 (nothing rescued)", got)
		}
		if got := countOps(t, r, r.NodeID()); got != 0 {
			t.Fatalf("new ops = %d, want 0", got)
		}
		if seq, _ := loadVectorEntry(app.DB(), oldID); seq != 1 {
			t.Fatalf("vector[old] = %d, want 1 (clamped to local history)", seq)
		}
	})

	t.Run("tombstones payload and files survive the re-emit", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		col := makeTestCollection(t, app, "posts") // seq 1

		rec := core.NewRecord(col)
		rec.Set("title", "doomed")
		if err := app.Save(rec); err != nil { // seq 2
			t.Fatal(err)
		}
		if err := app.Delete(rec); err != nil { // seq 3, a tombstone
			t.Fatal(err)
		}
		// a synthetic op with a files map, as blob-carrying ops have
		fileOp := &op{
			SrcNode: oldID, SrcSeq: 4, HLC: r.clock.Now(), Type: opUpsert,
			ColID: col.Id, ColName: col.Name, RecordID: "reczzzzzzzzzzzz",
			Payload: json.RawMessage(`{"id":"reczzzzzzzzzzzz","title":"blob"}`),
			Files:   map[string][]string{"attachment": {"a.png", "b.png"}},
		}
		if err := insertOp(app.DB(), fileOp); err != nil {
			t.Fatal(err)
		}
		before := srcRows(t, r, oldID)

		if err := r.adoptFreshNodeID(0); err != nil {
			t.Fatal(err)
		}
		after := srcRows(t, r, r.NodeID())
		if len(after) != len(before) {
			t.Fatalf("re-emitted %d ops, want %d", len(after), len(before))
		}
		for i := range after {
			if after[i].OpType != before[i].OpType ||
				after[i].HLC != before[i].HLC ||
				after[i].RecordID != before[i].RecordID ||
				after[i].Payload != before[i].Payload ||
				after[i].Files != before[i].Files {
				t.Fatalf("op %d mutated:\n got %+v\nwant %+v", i, after[i], before[i])
			}
		}
		if after[2].OpType != opDelete {
			t.Fatalf("tombstone lost: %+v", after[2])
		}
		if !strings.Contains(after[3].Files, "a.png") {
			t.Fatalf("files map lost: %q", after[3].Files)
		}
	})

	t.Run("other peers history and vector entries are untouched", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		makeTestCollection(t, app, "posts")
		for i := int64(1); i <= 2; i++ {
			if err := insertOp(app.DB(), &op{
				SrcNode: "peerZ0000000001", SrcSeq: i, HLC: encodeHLC(uint64(i), 0),
				Type: opUpsert, ColID: "x", ColName: "x", RecordID: "y",
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := setState(app.DB(), stateVectorPrefix+"peerZ0000000001", "2"); err != nil {
			t.Fatal(err)
		}

		if err := r.adoptFreshNodeID(0); err != nil {
			t.Fatal(err)
		}
		if got := countOps(t, r, "peerZ0000000001"); got != 2 {
			t.Fatalf("peerZ ops = %d, want 2", got)
		}
		if seq, _ := loadVectorEntry(app.DB(), "peerZ0000000001"); seq != 2 {
			t.Fatalf("vector[peerZ] = %d, want 2", seq)
		}
	})

	t.Run("version rows superseded by another peer keep their attribution", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		col := makeTestCollection(t, app, "posts")
		rec := core.NewRecord(col)
		rec.Set("title", "mine")
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
		// a NEWER write from another peer overwrote the record locally
		newer := &op{
			SrcNode: "peerZ0000000001", SrcSeq: 1, HLC: r.clock.Now(),
			Type: opUpsert, ColID: col.Id, ColName: col.Name, RecordID: rec.Id,
			Payload: json.RawMessage(`{"id":"` + rec.Id + `","title":"theirs"}`),
		}
		if err := r.applyOp(newer); err != nil {
			t.Fatal(err)
		}

		if err := r.adoptFreshNodeID(0); err != nil {
			t.Fatal(err)
		}
		ver, _ := getVersion(app.DB(), col.Id, rec.Id)
		if ver == nil || ver.SrcNode != "peerZ0000000001" {
			t.Fatalf("superseded version must keep the peer attribution, got %+v", ver)
		}
		// while the collection op we still own follows the new id
		colVer, _ := getVersion(app.DB(), collectionsColID, col.Id)
		if colVer == nil || colVer.SrcNode != r.NodeID() {
			t.Fatalf("owned collection version must follow the new id, got %+v", colVer)
		}
		_ = oldID
	})

	t.Run("member row with the original owners URL is preserved", func(t *testing.T) {
		// fresh clone, first boot: the old id's member row still points at
		// the ORIGINAL node and must survive so we can sync from it
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		if err := upsertMember(app.DB(), &member{
			NodeID: oldID, URL: "http://the-original:8090", Reachable: true, LastSeen: nowStr(),
		}); err != nil {
			t.Fatal(err)
		}

		if err := r.adoptFreshNodeID(0); err != nil {
			t.Fatal(err)
		}
		m, _ := getMember(app.DB(), oldID)
		if m == nil || m.URL != "http://the-original:8090" {
			t.Fatalf("original owner's URL must survive, got %+v", m)
		}
	})

	t.Run("member row clobbered with our own URL is cleared", func(t *testing.T) {
		// the clone ran before: its startBackground overwrote the old id's
		// row with the CLONE's URL, which would point peers at ourselves
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		if err := upsertMember(app.DB(), &member{
			NodeID: oldID, URL: r.cfg.NodeURL, Reachable: true, LastSeen: nowStr(),
		}); err != nil {
			t.Fatal(err)
		}

		if err := r.adoptFreshNodeID(0); err != nil {
			t.Fatal(err)
		}
		m, _ := getMember(app.DB(), oldID)
		if m == nil || m.URL != "" || m.Reachable {
			t.Fatalf("clobbered URL must be cleared, got %+v", m)
		}
	})

	t.Run("writes after adoption continue the new sequence", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		col := makeTestCollection(t, app, "posts") // seq 1
		rec := core.NewRecord(col)
		rec.Set("title", "before")
		if err := app.Save(rec); err != nil { // seq 2
			t.Fatal(err)
		}

		if err := r.adoptFreshNodeID(1); err != nil { // rescues seq 2 as new seq 1
			t.Fatal(err)
		}
		newID := r.NodeID()

		rec2 := core.NewRecord(col)
		rec2.Set("title", "after")
		if err := app.Save(rec2); err != nil {
			t.Fatal(err)
		}
		last := lastOps(t, r, 1)[0]
		if last.SrcNode != newID || last.SrcSeq != 2 {
			t.Fatalf("post-heal write = %s/%d, want %s/2 (no gaps, no collisions)",
				last.SrcNode, last.SrcSeq, newID)
		}
	})

	t.Run("re-emitted ops are visible to pulls and pushes", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		col := makeTestCollection(t, app, "posts")
		rec := core.NewRecord(col)
		rec.Set("title", "rescue me")
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}

		headBefore, err := maxRowID(app.DB())
		if err != nil {
			t.Fatal(err)
		}
		if err := r.adoptFreshNodeID(1); err != nil {
			t.Fatal(err)
		}
		newID := r.NodeID()

		// anti-entropy view of a peer that holds the acked prefix
		ops, _, err := opsAfterVector(app.DB(), map[string]int64{oldID: 1}, 100)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, o := range ops {
			if o.SrcNode == newID && o.RecordID == rec.Id {
				found = true
			}
		}
		if !found {
			t.Fatalf("rescued op missing from the anti-entropy view: %+v", ops)
		}

		// push view: startBackground snapshots the oplog head BEFORE the
		// heal, so re-emitted rows must sit above that head
		pushOps, _, err := opsAfterRowID(app.DB(), headBefore, 100)
		if err != nil {
			t.Fatal(err)
		}
		found = false
		for _, o := range pushOps {
			if o.SrcNode == newID && o.RecordID == rec.Id {
				found = true
			}
		}
		if !found {
			t.Fatalf("rescued op below the pre-heal push cursor: %+v", pushOps)
		}
	})
}

// ---------------------------------------------------------------------
// resolveDuplicateNodeID edge cases

func TestResolveDuplicateNodeIDEdgeCases(t *testing.T) {
	t.Run("garbage flag value with an unreachable seed still heals", func(t *testing.T) {
		app, r := newTestNodeCfgID(t, "nodeA0000000001", "http://127.0.0.1:1")
		if err := setState(app.DB(), stateDupNodePending, "not-a-number"); err != nil {
			t.Fatal(err)
		}
		r.resolveDuplicateNodeID()
		if r.NodeID() == "nodeA0000000001" {
			t.Fatal("a corrupt flag must still trigger the regeneration")
		}
	})

	t.Run("flag without a seed configured still heals", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001") // no SeedURL
		if err := setState(app.DB(), stateDupNodePending, "0"); err != nil {
			t.Fatal(err)
		}
		r.resolveDuplicateNodeID()
		if r.NodeID() == "nodeA0000000001" {
			t.Fatal("the flag path must not depend on a SeedURL")
		}
	})

	t.Run("flag with unknown ack refetches it from the live seed", func(t *testing.T) {
		seedApp, seed := newTestNode(t, "nodeA0000000001")
		makeTestCollection(t, seedApp, "posts") // seed local_seq = 1
		srv := serveReplicator(t, seed)

		app, r := newTestNodeCfgID(t, "nodeA0000000001", srv.URL)
		// the clone holds the shared op plus one local write
		if err := insertOp(app.DB(), &op{
			SrcNode: "nodeA0000000001", SrcSeq: 1, HLC: encodeHLC(1, 0),
			Type: opUpsert, ColID: "x", ColName: "x", RecordID: "shared",
		}); err != nil {
			t.Fatal(err)
		}
		if err := insertOp(app.DB(), &op{
			SrcNode: "nodeA0000000001", SrcSeq: 2, HLC: encodeHLC(2, 0),
			Type: opUpsert, ColID: "x", ColName: "x", RecordID: "local-only",
		}); err != nil {
			t.Fatal(err)
		}
		if err := setState(app.DB(), stateDupNodePending, "-1"); err != nil {
			t.Fatal(err)
		}

		r.resolveDuplicateNodeID()

		newID := r.NodeID()
		if newID == "nodeA0000000001" {
			t.Fatal("identity must be regenerated")
		}
		// ack fetched live = 1, so exactly the local-only op is rescued
		rows := srcRows(t, r, newID)
		if len(rows) != 1 || rows[0].RecordID != "local-only" {
			t.Fatalf("rescued = %+v, want just the local-only op", rows)
		}
		if seq, _ := loadVectorEntry(app.DB(), "nodeA0000000001"); seq != 1 {
			t.Fatalf("vector[old] = %d, want the fetched ack 1", seq)
		}
	})

	t.Run("flag takes precedence over the probe and heals once", func(t *testing.T) {
		pings := 0
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/ping", func(w http.ResponseWriter, req *http.Request) {
			pings++
			_ = json.NewEncoder(w).Encode(map[string]string{"node_id": "nodeA0000000001"})
		})
		mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(&joinResponse{NodeID: "nodeA0000000001",
				Vector: map[string]int64{"nodeA0000000001": 0}})
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		app, r := newTestNodeCfgID(t, "nodeA0000000001", srv.URL)
		if err := setState(app.DB(), stateDupNodePending, "0"); err != nil {
			t.Fatal(err)
		}

		r.resolveDuplicateNodeID()
		first := r.NodeID()
		if first == "nodeA0000000001" {
			t.Fatal("flag path must heal")
		}
		if pings != 0 {
			t.Fatalf("the probe must not run when the flag already decides: %d pings", pings)
		}

		// a second resolve (next restart, flag now cleared) probes, sees a
		// foreign id... which no longer matches ours - and does nothing
		r.resolveDuplicateNodeID()
		if r.NodeID() != first {
			t.Fatal("an already-healed node must not heal again")
		}
	})

	t.Run("seed url equal to own node url skips the probe", func(t *testing.T) {
		// self-seeding cannot prove a duplicate (a stale process of THIS
		// node would answer with our id); the join backstop covers it
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/ping", func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{"node_id": "nodeA0000000001"})
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			NodeURL:       srv.URL,
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})
		r.resolveDuplicateNodeID()
		if r.NodeID() != "nodeA0000000001" {
			t.Fatal("self-seeded probe must not regenerate the identity")
		}
	})

	t.Run("empty ping response id is ignored", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/ping", func(w http.ResponseWriter, req *http.Request) {
			_, _ = w.Write([]byte(`{}`))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		_, r := newTestNodeCfgID(t, "nodeA0000000001", srv.URL)
		r.resolveDuplicateNodeID()
		if r.NodeID() != "nodeA0000000001" {
			t.Fatal("an empty ping id must not trigger a heal")
		}
	})
}

// newTestNodeCfgID is a shorthand for a node with a pinned id + seed.
func newTestNodeCfgID(t *testing.T, nodeID, seedURL string) (*tests.TestApp, *Replicator) {
	t.Helper()
	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID:        nodeID,
		SeedURL:       seedURL,
		ClusterSecret: testSecret,
	})
	return app, r
}

// ---------------------------------------------------------------------
// fetchSeedAck

func TestFetchSeedAck(t *testing.T) {
	t.Run("same-id seed answers with its vector instead of 409", func(t *testing.T) {
		seedApp, seed := newTestNode(t, "nodeA0000000001")
		makeTestCollection(t, seedApp, "posts")
		srv := serveReplicator(t, seed)

		_, r := newTestNodeCfgID(t, "nodeA0000000001", srv.URL)
		if ack := r.fetchSeedAck(); ack != 1 {
			t.Fatalf("ack = %d, want the seed's own seq 1", ack)
		}
		members, _ := listMembers(seedApp.DB(), true)
		if len(members) != 1 {
			t.Fatalf("the ack probe must not register a member, got %d", len(members))
		}
	})

	t.Run("unreachable seed yields -1", func(t *testing.T) {
		_, r := newTestNodeCfgID(t, "nodeA0000000001", "http://127.0.0.1:1")
		if ack := r.fetchSeedAck(); ack != -1 {
			t.Fatalf("ack = %d, want -1", ack)
		}
	})

	t.Run("missing vector yields -1", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(&joinResponse{NodeID: "nodeA0000000001"})
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		_, r := newTestNodeCfgID(t, "nodeA0000000001", srv.URL)
		if ack := r.fetchSeedAck(); ack != -1 {
			t.Fatalf("ack = %d, want -1", ack)
		}
	})

	t.Run("no seed configured yields -1", func(t *testing.T) {
		_, r := newTestNode(t, "nodeA0000000001")
		if ack := r.fetchSeedAck(); ack != -1 {
			t.Fatalf("ack = %d, want -1", ack)
		}
	})
}

// ---------------------------------------------------------------------
// old-seed compatibility (no false positives)

func TestJoinClusterOldSeedNoFalsePositive(t *testing.T) {
	// an old-version seed with a DIFFERENT id returns no instance id -
	// that must never be mistaken for a duplicate
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(&joinResponse{
			NodeID: "oldseed00000001",
			Vector: map[string]int64{"oldseed00000001": 7},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	app, r := newTestNodeCfgID(t, "nodeB0000000001", srv.URL)
	resp, err := r.joinCluster()
	if err != nil {
		t.Fatalf("join against an old seed must work: %v", err)
	}
	if resp.NodeID != "oldseed00000001" {
		t.Fatalf("resp = %+v", resp)
	}
	if v, _ := getState(app.DB(), stateDupNodePending); v != "" {
		t.Fatalf("no duplicate here, but flag = %q", v)
	}
}

// ---------------------------------------------------------------------
// full clone lifecycle against the real handlers

func TestCloneLifecycleEndToEnd(t *testing.T) {
	// original node A with a collection and two records (ops X:1..3)
	appA, rA := newTestNode(t, "nodeAlife000001")
	oldID := rA.NodeID()
	colA := makeTestCollection(t, appA, "posts")
	for _, title := range []string{"one", "two"} {
		rec := core.NewRecord(colA)
		rec.Set("title", title)
		if err := appA.Save(rec); err != nil {
			t.Fatal(err)
		}
	}
	srvA := serveReplicator(t, rA)

	// the operator "provisions" node B by copying A's data directory
	appB, rB := cloneNode(t, rA, srvA.URL)
	if rB.NodeID() != oldID {
		t.Fatalf("clone setup broken: %s vs %s", rB.NodeID(), oldID)
	}

	// B takes an offline write before anyone notices - captured under the
	// duplicated id as seq 4, a sequence A will also hand out eventually
	colB, err := appB.FindCollectionByNameOrId("posts")
	if err != nil {
		t.Fatal(err)
	}
	recB := core.NewRecord(colB)
	recB.Set("title", "written on the clone")
	if err := appB.Save(recB); err != nil {
		t.Fatal(err)
	}
	if got := countOps(t, rB, oldID); got != 4 {
		t.Fatalf("clone ops under %s = %d, want 4", oldID, got)
	}

	// B "restarts": the pre-serve probe hits A, sees its own id answered
	// by a foreign process and heals
	rB.resolveDuplicateNodeID()
	newID := rB.NodeID()
	if newID == oldID {
		t.Fatal("clone must regenerate its identity")
	}
	if got := countOps(t, rB, oldID); got != 3 {
		t.Fatalf("shared history under %s = %d, want 3", oldID, got)
	}
	rescued := srcRows(t, rB, newID)
	if len(rescued) != 1 || rescued[0].RecordID != recB.Id {
		t.Fatalf("rescued = %+v, want B's offline write", rescued)
	}

	// B joins for real and A registers it as a NEW member
	if _, err := rB.joinCluster(); err != nil {
		t.Fatalf("post-heal join failed: %v", err)
	}
	if m, _ := getMember(appA.DB(), newID); m == nil {
		t.Fatal("A must register the healed clone as a member")
	}
	membersA, _ := listMembers(appA.DB(), false)
	if len(membersA) != 2 {
		t.Fatalf("A members = %d, want 2 - the exact symptom this fixes", len(membersA))
	}

	// A syncs from B: the rescued offline write arrives under the new id
	vecA, err := rA.currentVector()
	if err != nil {
		t.Fatal(err)
	}
	opsForA, _, err := opsAfterVector(appB.DB(), vecA, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(opsForA) != 1 || opsForA[0].SrcNode != newID {
		t.Fatalf("A should receive exactly the rescued op, got %+v", opsForA)
	}
	if err := rA.ingestOps(opsForA); err != nil {
		t.Fatal(err)
	}
	for _, o := range opsForA {
		if err := rA.applyOp(o); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := appA.FindRecordById(colA, recB.Id); err != nil {
		t.Fatal("B's offline write must reach A after the heal")
	}

	// A now hands out seq 4 itself - the sequence B's offline write used
	// to occupy. B pulls it without any primary-key collision.
	recA := core.NewRecord(colA)
	recA.Set("title", "written on the original")
	if err := appA.Save(recA); err != nil {
		t.Fatal(err)
	}
	vecB, err := rB.currentVector()
	if err != nil {
		t.Fatal(err)
	}
	opsForB, _, err := opsAfterVector(appA.DB(), vecB, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(opsForB) != 1 || opsForB[0].SrcNode != oldID || opsForB[0].SrcSeq != 4 {
		t.Fatalf("B should receive A's seq-4 op, got %+v", opsForB)
	}
	if err := rB.ingestOps(opsForB); err != nil {
		t.Fatal(err)
	}
	for _, o := range opsForB {
		if err := rB.applyOp(o); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := appB.FindRecordById(colB, recA.Id); err != nil {
		t.Fatal("A's post-heal write must reach B")
	}
	if seq, _ := loadVectorEntry(appB.DB(), oldID); seq != 4 {
		t.Fatalf("B vector[%s] = %d, want 4", oldID, seq)
	}

	// both sides converged on all four records
	for name, app := range map[string]core.App{"A": appA, "B": appB} {
		var n int
		if err := app.DB().NewQuery(`SELECT COUNT(*) FROM posts`).Row(&n); err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			t.Fatalf("node %s holds %d records, want 4", name, n)
		}
	}
}

// TestSimultaneousCloneBackstop covers the twin-start case the probe
// cannot see (neither process was serving when the other probed): the
// join is rejected with 409, the clone flags itself, and the next start
// heals from the flag - including a live re-fetch of the ack.
func TestSimultaneousCloneBackstop(t *testing.T) {
	appA, rA := newTestNode(t, "nodeAtwin000001")
	oldID := rA.NodeID()
	colA := makeTestCollection(t, appA, "posts")
	rec := core.NewRecord(colA)
	rec.Set("title", "seed data")
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	srvA := serveReplicator(t, rA)

	appB, rB := cloneNode(t, rA, srvA.URL)
	colB, err := appB.FindCollectionByNameOrId("posts")
	if err != nil {
		t.Fatal(err)
	}
	offline := core.NewRecord(colB)
	offline.Set("title", "offline on the twin")
	if err := appB.Save(offline); err != nil {
		t.Fatal(err)
	}

	// the probe was missed (A wasn't up yet in the real scenario); B goes
	// straight to the join and hits the server-side backstop
	_, err = rB.joinCluster()
	if !errors.Is(err, errDuplicateNodeID) {
		t.Fatalf("err = %v, want errDuplicateNodeID", err)
	}
	if v, _ := getState(appB.DB(), stateDupNodePending); v != "-1" {
		t.Fatalf("flag = %q, want -1 (the 409 carries no ack)", v)
	}
	if !hasEvent(rA, EventDuplicateNode) {
		t.Fatal("the seed must record the duplicate on its event timeline")
	}
	membersA, _ := listMembers(appA.DB(), false)
	if len(membersA) != 1 {
		t.Fatalf("A registered the duplicate: %d members", len(membersA))
	}

	// B restarts: the flag path re-fetches the ack live and heals
	rB.resolveDuplicateNodeID()
	newID := rB.NodeID()
	if newID == oldID {
		t.Fatal("flagged twin must regenerate on restart")
	}
	rescued := srcRows(t, rB, newID)
	if len(rescued) != 1 || rescued[0].RecordID != offline.Id {
		t.Fatalf("rescued = %+v, want the twin's offline write", rescued)
	}
	if v, _ := getState(appB.DB(), stateDupNodePending); v != "" {
		t.Fatalf("flag must be cleared after the heal: %q", v)
	}

	// and the rejoin now succeeds
	if _, err := rB.joinCluster(); err != nil {
		t.Fatalf("post-heal join failed: %v", err)
	}
	if m, _ := getMember(appA.DB(), newID); m == nil {
		t.Fatal("A must register the healed twin")
	}
}

// TestCloneOverlapWindowKeepsLocalData documents the one inherently
// ambiguous case: the clone wrote offline at sequence numbers the
// original ALSO handed out before the heal. Those colliding writes
// cannot be told apart from the shared history via sequence numbers, so
// they stay local (nothing is deleted or corrupted, the record simply
// doesn't replicate) and the rest of the cluster keeps working.
func TestCloneOverlapWindowKeepsLocalData(t *testing.T) {
	appA, rA := newTestNode(t, "nodeAover000001")
	oldID := rA.NodeID()
	colA := makeTestCollection(t, appA, "posts")
	rec := core.NewRecord(colA)
	rec.Set("title", "shared")
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	srvA := serveReplicator(t, rA)

	appB, rB := cloneNode(t, rA, srvA.URL)
	colB, err := appB.FindCollectionByNameOrId("posts")
	if err != nil {
		t.Fatal(err)
	}

	// BOTH sides write before the heal: seq 3 exists on A and on B with
	// different content
	recB := core.NewRecord(colB)
	recB.Set("title", "clone seq 3")
	if err := appB.Save(recB); err != nil {
		t.Fatal(err)
	}
	recA := core.NewRecord(colA)
	recA.Set("title", "original seq 3")
	if err := appA.Save(recA); err != nil {
		t.Fatal(err)
	}

	rB.resolveDuplicateNodeID()
	newID := rB.NodeID()
	if newID == oldID {
		t.Fatal("clone must still regenerate its identity")
	}

	// the colliding write is indistinguishable from acked history: it
	// survives locally but is not re-emitted
	if _, err := appB.FindRecordById(colB, recB.Id); err != nil {
		t.Fatal("the colliding local write must never be deleted")
	}
	if got := countOps(t, rB, newID); got != 0 {
		t.Fatalf("colliding write must not be re-emitted, got %d ops", got)
	}

	// the cluster stays functional: a NEW write on B replicates normally
	if _, err := rB.joinCluster(); err != nil {
		t.Fatal(err)
	}
	recB2 := core.NewRecord(colB)
	recB2.Set("title", "post-heal write")
	if err := appB.Save(recB2); err != nil {
		t.Fatal(err)
	}
	vecA, err := rA.currentVector()
	if err != nil {
		t.Fatal(err)
	}
	opsForA, _, err := opsAfterVector(appB.DB(), vecA, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(opsForA) != 1 || opsForA[0].RecordID != recB2.Id {
		t.Fatalf("post-heal traffic = %+v, want just the new write", opsForA)
	}
}
