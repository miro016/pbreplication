package pbreplication

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

func countOps(t *testing.T, r *Replicator, srcNode string) int {
	t.Helper()
	var n int
	if err := r.app.DB().NewQuery(`SELECT COUNT(*) FROM _repl_oplog WHERE src_node = {:s}`).
		Bind(dbx.Params{"s": srcNode}).Row(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAdoptFreshNodeID(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	oldID := r.NodeID()

	// 1 collection op + 3 record ops under the old id
	col := makeTestCollection(t, app, "posts")
	recIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		rec := core.NewRecord(col)
		rec.Set("title", "t")
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
		recIDs = append(recIDs, rec.Id)
	}
	if got := countOps(t, r, oldID); got != 4 {
		t.Fatalf("setup: expected 4 ops under %s, got %d", oldID, got)
	}
	rescued := lastOps(t, r, 2) // seqs 3 and 4, beyond the ack below

	// the "original owner" acknowledges the shared history up to seq 2
	if err := r.adoptFreshNodeID(2); err != nil {
		t.Fatal(err)
	}
	newID := r.NodeID()
	if newID == oldID || newID == "" {
		t.Fatalf("expected a fresh node id, got %q", newID)
	}
	if v, _ := getState(app.DB(), stateNodeID); v != newID {
		t.Fatalf("persisted node_id = %q, want %q", v, newID)
	}

	// unacknowledged ops moved under the new id with fresh sequences
	if got := countOps(t, r, oldID); got != 2 {
		t.Fatalf("ops left under old id = %d, want 2", got)
	}
	var rows []oplogRow
	if err := app.DB().NewQuery(`SELECT rowid, * FROM _repl_oplog WHERE src_node = {:s} ORDER BY src_seq`).
		Bind(dbx.Params{"s": newID}).All(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("re-emitted ops = %d, want 2", len(rows))
	}
	for i, row := range rows {
		if row.SrcSeq != int64(i+1) {
			t.Fatalf("re-emitted op %d has seq %d, want %d", i, row.SrcSeq, i+1)
		}
		if row.HLC != rescued[i].HLC || row.RecordID != rescued[i].RecordID {
			t.Fatalf("re-emitted op %d lost its HLC/record: %+v vs %+v", i, row, rescued[i])
		}
	}

	// bookkeeping: local_seq restarts at the re-emitted count, the old id
	// becomes a peer covered up to the ack, the pending flag is cleared
	if v, _ := getState(app.DB(), stateLocalSeq); v != "2" {
		t.Fatalf("local_seq = %q, want 2", v)
	}
	if seq, _ := loadVectorEntry(app.DB(), oldID); seq != 2 {
		t.Fatalf("vector[%s] = %d, want 2", oldID, seq)
	}
	if v, _ := getState(app.DB(), stateDupNodePending); v != "" {
		t.Fatalf("dup flag not cleared: %q", v)
	}

	// LWW attribution of the rescued writes follows the new id
	ver, err := getVersion(app.DB(), col.Id, recIDs[2])
	if err != nil || ver == nil {
		t.Fatalf("version row missing: %v", err)
	}
	if ver.SrcNode != newID {
		t.Fatalf("rescued version src_node = %q, want %q", ver.SrcNode, newID)
	}
	// ...while acknowledged history keeps the old attribution
	ver, _ = getVersion(app.DB(), col.Id, recIDs[0])
	if ver == nil || ver.SrcNode != oldID {
		t.Fatalf("acknowledged version src_node = %+v, want %q", ver, oldID)
	}

	// unknown ack (-1) re-emits everything still held under the old id
	if err := r.adoptFreshNodeID(-1); err != nil {
		t.Fatal(err)
	}
	thirdID := r.NodeID()
	if got := countOps(t, r, newID); got != 0 {
		t.Fatalf("ops left under %s = %d, want 0", newID, got)
	}
	if got := countOps(t, r, thirdID); got != 2 {
		t.Fatalf("ops under %s = %d, want 2", thirdID, got)
	}
}

func TestJoinClusterDetectsDuplicate(t *testing.T) {
	newSeed := func(t *testing.T, handler http.HandlerFunc) *httptest.Server {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/join", handler)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		return srv
	}

	t.Run("seed echoes our id with a foreign instance id", func(t *testing.T) {
		srv := newSeed(t, func(w http.ResponseWriter, req *http.Request) {
			json.NewEncoder(w).Encode(&joinResponse{
				NodeID:     "nodeA0000000001",
				InstanceID: "someotherprocess",
				Vector:     map[string]int64{"nodeA0000000001": 5},
			})
		})
		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})

		_, err := r.joinCluster()
		if !errors.Is(err, errDuplicateNodeID) {
			t.Fatalf("err = %v, want errDuplicateNodeID", err)
		}
		if v, _ := getState(app.DB(), stateDupNodePending); v != "5" {
			t.Fatalf("dup flag = %q, want the seed ack 5", v)
		}
	})

	t.Run("duplicate-aware seed rejects with 409", func(t *testing.T) {
		srv := newSeed(t, func(w http.ResponseWriter, req *http.Request) {
			http.Error(w, `{"message":"duplicate node id"}`, http.StatusConflict)
		})
		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})

		_, err := r.joinCluster()
		if !errors.Is(err, errDuplicateNodeID) {
			t.Fatalf("err = %v, want errDuplicateNodeID", err)
		}
		if v, _ := getState(app.DB(), stateDupNodePending); v != "-1" {
			t.Fatalf("dup flag = %q, want -1 (unknown ack)", v)
		}
	})

	t.Run("self-join through a loop is not a duplicate", func(t *testing.T) {
		app := newTestAppOnly(t)
		var r *Replicator
		srv := newSeed(t, func(w http.ResponseWriter, req *http.Request) {
			// e.g. a load balancer routed the join back to ourselves
			json.NewEncoder(w).Encode(&joinResponse{
				NodeID:     "nodeA0000000001",
				InstanceID: r.instanceID,
			})
		})
		r = newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})

		if _, err := r.joinCluster(); err != nil {
			t.Fatalf("self-join must not error: %v", err)
		}
		if v, _ := getState(app.DB(), stateDupNodePending); v != "" {
			t.Fatalf("dup flag set on a self-join: %q", v)
		}
	})
}

func TestResolveDuplicateNodeID(t *testing.T) {
	t.Run("pending flag regenerates the identity", func(t *testing.T) {
		app, r := newTestNode(t, "nodeA0000000001")
		oldID := r.NodeID()
		if err := setState(app.DB(), stateDupNodePending, "0"); err != nil {
			t.Fatal(err)
		}

		r.resolveDuplicateNodeID()

		if r.NodeID() == oldID {
			t.Fatal("identity must be regenerated when the dup flag is set")
		}
		if v, _ := getState(app.DB(), stateDupNodePending); v != "" {
			t.Fatalf("dup flag not cleared: %q", v)
		}
	})

	t.Run("live probe detects the seed answering with our id", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/ping", func(w http.ResponseWriter, req *http.Request) {
			json.NewEncoder(w).Encode(map[string]string{"node_id": "nodeA0000000001"})
		})
		mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
			json.NewEncoder(w).Encode(&joinResponse{
				NodeID: "nodeA0000000001",
				Vector: map[string]int64{"nodeA0000000001": 0},
			})
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			NodeURL:       "http://nodeA.test:8090",
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})
		r.resolveDuplicateNodeID()

		newID := r.NodeID()
		if newID == "nodeA0000000001" {
			t.Fatal("identity must be regenerated when the seed answers with our id")
		}
		if v, _ := getState(app.DB(), stateNodeID); v != newID {
			t.Fatalf("persisted node_id = %q, want %q", v, newID)
		}
		if seq, err := loadVectorEntry(app.DB(), "nodeA0000000001"); err != nil || seq != 0 {
			t.Fatalf("vector[old] = %d (%v), want 0", seq, err)
		}
	})

	t.Run("different seed id is left alone", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/replication/ping", func(w http.ResponseWriter, req *http.Request) {
			json.NewEncoder(w).Encode(map[string]string{"node_id": "someothernode01"})
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       srv.URL,
			ClusterSecret: testSecret,
		})

		r.resolveDuplicateNodeID()

		if r.NodeID() != "nodeA0000000001" {
			t.Fatalf("identity changed without a duplicate: %q", r.NodeID())
		}
	})

	t.Run("unreachable seed is a no-op", func(t *testing.T) {
		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       "http://127.0.0.1:1", // nothing listens here
			ClusterSecret: testSecret,
		})

		r.resolveDuplicateNodeID()

		if r.NodeID() != "nodeA0000000001" {
			t.Fatalf("identity changed with the seed down: %q", r.NodeID())
		}
	})
}

func TestBootstrapStopsRetryingOnDuplicate(t *testing.T) {
	joins := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
		joins++
		http.Error(w, `{"message":"duplicate node id"}`, http.StatusConflict)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID:        "nodeA0000000001",
		SeedURL:       srv.URL,
		ClusterSecret: testSecret,
	})
	// a clone always carries the original's bootstrap marker
	if err := setState(app.DB(), stateBootstrapDone, nowStr()); err != nil {
		t.Fatal(err)
	}

	if err := r.bootstrapOrRejoin(); err != nil {
		t.Fatalf("duplicate detection must not bubble up as a retryable error: %v", err)
	}
	if joins != 1 {
		t.Fatalf("joins = %d, want exactly 1", joins)
	}
	if v, _ := getState(app.DB(), stateDupNodePending); v != "-1" {
		t.Fatalf("dup flag = %q, want -1", v)
	}
}
