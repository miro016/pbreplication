package pbreplication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/pocketbase/pocketbase/core"
)

func TestPrepareDBSnapshotVacuumAndCache(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	makeTestCollection(t, app, "posts")

	m, err := r.prepareDBSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if m.NodeID != r.nodeID || m.SizeBytes <= 0 || m.SHA256 == "" {
		t.Fatalf("manifest malformed: %+v", m)
	}
	if m.Vector[r.nodeID] == 0 {
		t.Fatal("manifest vector must include the serving node's own local seq")
	}

	path := r.dbSnapshotPath(m.ID)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
	if int64(len(b)) != m.SizeBytes {
		t.Fatalf("size mismatch: file %d, manifest %d", len(b), m.SizeBytes)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != m.SHA256 {
		t.Fatal("sha256 mismatch")
	}

	// the copy must be a valid SQLite DB containing the app's data
	db, err := core.DefaultDBConnect(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE name = '_repl_state'`).Row(&n); err != nil || n != 1 {
		t.Fatalf("snapshot is not a usable database copy: n=%d err=%v", n, err)
	}

	// second call within the TTL reuses the same snapshot
	m2, err := r.prepareDBSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if m2.ID != m.ID {
		t.Fatal("snapshot cache not reused within TTL")
	}
}

func TestDBSnapshotChunkHandler(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	makeTestCollection(t, app, "posts")

	m, err := r.prepareDBSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	full, _ := os.ReadFile(r.dbSnapshotPath(m.ID))

	get := func(query string, gzipAccept bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "http://n/api/replication/snapshot/db/chunk?"+query, nil)
		if gzipAccept {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		rec := httptest.NewRecorder()
		e := &core.RequestEvent{}
		e.Response = rec
		e.Request = req
		if err := r.handleDBSnapshotChunk(e); err != nil {
			// error responses are returned as ApiError; surface status via recorder
			t.Logf("handler err: %v", err)
		}
		return rec
	}

	// a middle chunk matches the file bytes
	rec := get(fmt.Sprintf("id=%s&offset=10&limit=100", m.ID), false)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if got := rec.Body.Bytes(); string(got) != string(full[10:110]) {
		t.Fatalf("chunk bytes mismatch (%d bytes)", len(got))
	}

	// the final chunk is truncated at EOF
	rec = get(fmt.Sprintf("id=%s&offset=%d&limit=1000000", m.ID, m.SizeBytes-5), false)
	if rec.Body.Len() != 5 {
		t.Fatalf("tail chunk length = %d, want 5", rec.Body.Len())
	}

	// gzip round-trip
	rec = get(fmt.Sprintf("id=%s&offset=0&limit=64", m.ID), true)
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("expected gzipped response")
	}

	// unknown id -> not found
	req := httptest.NewRequest(http.MethodGet, "http://n/api/replication/snapshot/db/chunk?id=nope&offset=0&limit=10", nil)
	e := &core.RequestEvent{}
	e.Response = httptest.NewRecorder()
	e.Request = req
	if err := r.handleDBSnapshotChunk(e); err == nil {
		t.Fatal("unknown snapshot id must error")
	}
}

// chunkServer serves a fixed payload with the chunk endpoint's wire
// contract and records the offsets requested.
func chunkServer(t *testing.T, payload []byte) (*httptest.Server, *[]int64) {
	t.Helper()
	var mu sync.Mutex
	offsets := &[]int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
		lim, _ := strconv.ParseInt(q.Get("limit"), 10, 64)
		mu.Lock()
		*offsets = append(*offsets, off)
		mu.Unlock()
		if off >= int64(len(payload)) {
			http.Error(w, "offset beyond end", http.StatusBadRequest)
			return
		}
		end := off + lim
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		w.Write(payload[off:end])
	}))
	t.Cleanup(srv.Close)
	return srv, offsets
}

func TestDownloadDBSnapshotResume(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")
	r.cfg.FullCopyChunkSize = 32 // force many chunks

	payload := []byte("The quick brown fox jumps over the lazy dog. 0123456789 repeated a few times to cross chunk borders!")
	sum := sha256.Sum256(payload)
	m := &dbSnapshotManifest{
		ID:        "snaptest00000001",
		NodeID:    "seed000000000001",
		SizeBytes: int64(len(payload)),
		SHA256:    hex.EncodeToString(sum[:]),
	}

	srv, offsets := chunkServer(t, payload)

	// pre-seed a partial download of the SAME snapshot -> must resume
	if err := os.MkdirAll(r.copyWorkDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.incomingDBPath(), payload[:40], 0o600); err != nil {
		t.Fatal(err)
	}
	sb, _ := json.Marshal(m)
	if err := os.WriteFile(r.incomingSidecarPath(), sb, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := r.downloadDBSnapshot(context.Background(), srv.URL, m); err != nil {
		t.Fatal(err)
	}

	got, _ := os.ReadFile(r.incomingDBPath())
	if string(got) != string(payload) {
		t.Fatalf("downloaded content mismatch (%d vs %d bytes)", len(got), len(payload))
	}
	if len(*offsets) == 0 || (*offsets)[0] != 40 {
		t.Fatalf("expected resume from offset 40, first requested offset: %v", *offsets)
	}

	// a mismatched sidecar (different snapshot) restarts from zero
	m2 := *m
	m2.ID = "snaptest00000002"
	*offsets = (*offsets)[:0]
	if err := r.downloadDBSnapshot(context.Background(), srv.URL, &m2); err != nil {
		t.Fatal(err)
	}
	if (*offsets)[0] != 0 {
		t.Fatalf("expected restart from offset 0, got %v", (*offsets)[0])
	}
}

func TestSanitizeCopiedDB(t *testing.T) {
	seedApp, seed := newTestNode(t, "seed000000000001")
	col := makeTestCollection(t, seedApp, "posts")
	rec := core.NewRecord(col)
	rec.Set("title", "hello")
	if err := seedApp.Save(rec); err != nil {
		t.Fatal(err)
	}
	// simulate node-local telemetry on the seed
	if _, err := seedApp.NonconcurrentDB().NewQuery(
		`INSERT INTO _repl_client_ips (ip) VALUES ('1.2.3.4')`).Execute(); err != nil {
		t.Fatal(err)
	}

	m, err := seed.prepareDBSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	// hand the snapshot to a "new node" replicator for sanitizing
	_, r := newTestNode(t, "nodeB0000000002")
	if err := os.MkdirAll(r.copyWorkDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	copied := r.incomingDBPath()
	b, _ := os.ReadFile(seed.dbSnapshotPath(m.ID))
	if err := os.WriteFile(copied, b, 0o600); err != nil {
		t.Fatal(err)
	}

	identity := &rescueState{NodeID: "nodeB0000000002", LocalSeq: 7, HLC: encodeHLC(999999, 3)}
	if err := r.sanitizeCopiedDB(copied, m, identity); err != nil {
		t.Fatal(err)
	}

	db, err := core.DefaultDBConnect(copied)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	assertState := func(key, want string) {
		t.Helper()
		got, err := getState(db, key)
		if err != nil || got != want {
			t.Fatalf("state %s = %q (err %v), want %q", key, got, err, want)
		}
	}
	assertState(stateNodeID, "nodeB0000000002")
	assertState(stateResyncPending, "")
	assertState(stateSnapshotResume, "")
	assertState(stateBlobBackfillPending, "1")
	assertState(stateVectorPrefix+"seed000000000001", fmt.Sprintf("%d", m.Vector["seed000000000001"]))

	if done, _ := getState(db, stateBootstrapDone); done == "" {
		t.Fatal("bootstrap_done must be set")
	}
	// local_seq floor: max(identity 7, seed's own ops in the copied oplog for OUR id (none))
	if seqStr, _ := getState(db, stateLocalSeq); seqStr != "7" {
		t.Fatalf("local_seq = %s, want 7", seqStr)
	}
	// no self vector entry
	if v, _ := getState(db, stateVectorPrefix+"nodeB0000000002"); v != "" {
		t.Fatalf("self vector entry must be removed, got %q", v)
	}
	// telemetry emptied
	var clients int
	_ = db.NewQuery(`SELECT COUNT(*) FROM _repl_client_ips`).Row(&clients)
	if clients != 0 {
		t.Fatalf("client telemetry not truncated: %d rows", clients)
	}
	// self member row exists
	if mrow, err := getMember(db, "nodeB0000000002"); err != nil || mrow == nil {
		t.Fatalf("self member row missing: %v", err)
	}
	// replicated data survived
	var posts int
	if err := db.NewQuery(`SELECT COUNT(*) FROM posts`).Row(&posts); err != nil || posts != 1 {
		t.Fatalf("replicated data lost: posts=%d err=%v", posts, err)
	}
	// excluded auth artifacts emptied (table exists in the PB schema)
	var mfas int
	if err := db.NewQuery(`SELECT COUNT(*) FROM _mfas`).Row(&mfas); err == nil && mfas != 0 {
		t.Fatalf("_mfas not truncated: %d", mfas)
	}
}

func TestDecideBootstrapStrategy(t *testing.T) {
	// fresh: an empty data dir
	appFresh := newTestAppOnly(t)
	rFresh := newTestNodeCfg(t, appFresh, Config{
		NodeID: "nodeF0000000001", ClusterSecret: testSecret, SeedURL: "http://seed.test",
	})
	emptyDir := t.TempDir()
	fakeApp := &dataDirApp{App: appFresh, dir: emptyDir}
	if s, err := rFresh.decideBootstrapStrategy(fakeApp); err != nil || s != strategyFreshCopy {
		t.Fatalf("empty dir: strategy=%v err=%v, want freshCopy", s, err)
	}

	// existing DB without resync flag -> none
	if s, err := rFresh.decideBootstrapStrategy(appFresh); err != nil || s != strategyNone {
		t.Fatalf("existing db: strategy=%v err=%v, want none", s, err)
	}

	// resync_pending set -> resync copy
	if err := setState(appFresh.NonconcurrentDB(), stateResyncPending, nowStr()); err != nil {
		t.Fatal(err)
	}
	if s, err := rFresh.decideBootstrapStrategy(appFresh); err != nil || s != strategyResyncCopy {
		t.Fatalf("flagged db: strategy=%v err=%v, want resyncCopy", s, err)
	}
}

// dataDirApp overrides DataDir for strategy tests.
type dataDirApp struct {
	core.App
	dir string
}

func (a *dataDirApp) DataDir() string { return a.dir }

func TestRescueAndReplayLocalOps(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "posts")

	freshHLC := r.clock.Now()
	staleHLC := encodeHLC(1000, 0)

	winPayload, _ := json.Marshal(map[string]any{"id": "recwin000000001", "title": "offline-write"})
	losePayload, _ := json.Marshal(map[string]any{"id": "reclose00000001", "title": "stale-offline"})

	rescue := &rescueState{
		NodeID:   r.nodeID,
		LocalSeq: 5,
		HLC:      freshHLC,
		Ops: []*op{
			{SrcNode: r.nodeID, SrcSeq: 4, HLC: freshHLC, Type: opUpsert,
				ColID: col.Id, ColName: col.Name, RecordID: "recwin000000001", Payload: winPayload},
			{SrcNode: r.nodeID, SrcSeq: 5, HLC: staleHLC, Type: opUpsert,
				ColID: col.Id, ColName: col.Name, RecordID: "reclose00000001", Payload: losePayload},
		},
	}

	// the cluster already wrote reclose... NEWER than the rescued op
	clusterRec := core.NewRecord(col)
	clusterRec.Id = "reclose00000001"
	clusterRec.Set("title", "cluster-newer")
	if err := app.Save(clusterRec); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(r.copyWorkDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(rescue)
	if err := os.WriteFile(r.rescuePath(), b, 0o600); err != nil {
		t.Fatal(err)
	}

	r.replayRescuedOps()

	// winning op applied + record present
	winRec, err := app.FindRecordById(col, "recwin000000001")
	if err != nil {
		t.Fatalf("rescued write lost: %v", err)
	}
	if winRec.GetString("title") != "offline-write" {
		t.Fatalf("rescued write content wrong: %q", winRec.GetString("title"))
	}

	// losing op skipped - cluster's newer value survives
	loseRec, err := app.FindRecordById(col, "reclose00000001")
	if err != nil {
		t.Fatal(err)
	}
	if loseRec.GetString("title") != "cluster-newer" {
		t.Fatalf("stale rescued op overwrote newer cluster write: %q", loseRec.GetString("title"))
	}

	// the replayed op was re-emitted with a NEW sequence and its ORIGINAL HLC
	ops, _, err := opsAfterRowID(app.DB(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range ops {
		if o.RecordID == "recwin000000001" && o.SrcNode == r.nodeID {
			found = true
			if o.HLC != freshHLC {
				t.Fatalf("replayed op HLC restamped: %s != %s", o.HLC, freshHLC)
			}
			if o.SrcSeq == 4 {
				t.Fatal("replayed op must get a fresh sequence number")
			}
		}
	}
	if !found {
		t.Fatal("replayed op not re-emitted into the oplog")
	}

	// rescue file consumed
	if _, err := os.Stat(r.rescuePath()); !os.IsNotExist(err) {
		t.Fatal("rescue file must be removed after replay")
	}
}

func TestFullCopyFallsBackOnOldPeer(t *testing.T) {
	// a peer that has no /snapshot/db endpoint answers 404
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.NotFound(w, req)
	}))
	defer srv.Close()

	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID: "nodeX0000000001", ClusterSecret: testSecret, SeedURL: srv.URL,
	})

	err := r.tryFullCopy(context.Background(), app, strategyFreshCopy)
	if !errors.Is(err, errFullCopyUnsupported) {
		t.Fatalf("expected errFullCopyUnsupported, got %v", err)
	}
}

func TestFullCopyEndToEnd(t *testing.T) {
	// seed with data
	seedApp, seed := newTestNode(t, "seed000000000001")
	col := makeTestCollection(t, seedApp, "articles")
	for i := 0; i < 25; i++ {
		rec := core.NewRecord(col)
		rec.Set("title", fmt.Sprintf("article-%d", i))
		if err := seedApp.Save(rec); err != nil {
			t.Fatal(err)
		}
	}

	// expose the seed's snapshot endpoints over real HTTP
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/snapshot/db", func(w http.ResponseWriter, req *http.Request) {
		m, err := seed.prepareDBSnapshot()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("/api/replication/snapshot/db/chunk", func(w http.ResponseWriter, req *http.Request) {
		e := &core.RequestEvent{}
		e.Response = w
		e.Request = req
		if err := seed.handleDBSnapshotChunk(e); err != nil {
			http.Error(w, err.Error(), 500)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// "new node": empty data dir, copy pre-bootstrap
	newDir := t.TempDir()
	joinApp := newTestAppOnly(t)
	r := newTestNodeCfg(t, joinApp, Config{
		NodeID: "nodeN0000000001", ClusterSecret: testSecret, SeedURL: srv.URL,
	})
	fake := &dataDirApp{App: joinApp, dir: newDir}

	if err := r.tryFullCopy(context.Background(), fake, strategyFreshCopy); err != nil {
		t.Fatal(err)
	}

	// the installed database is a valid PB db owned by the new node
	installed := filepath.Join(newDir, "data.db")
	db, err := core.DefaultDBConnect(installed)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var articles int
	if err := db.NewQuery(`SELECT COUNT(*) FROM articles`).Row(&articles); err != nil || articles != 25 {
		t.Fatalf("copied data wrong: articles=%d err=%v", articles, err)
	}
	if id, _ := getState(db, stateNodeID); id != "nodeN0000000001" {
		t.Fatalf("copied db identity = %q", id)
	}
	if done, _ := getState(db, stateBootstrapDone); done == "" {
		t.Fatal("copied db must be marked bootstrapped")
	}
	var migrations int
	if err := db.NewQuery(`SELECT COUNT(*) FROM _migrations`).Row(&migrations); err != nil || migrations == 0 {
		t.Fatalf("_migrations history must travel with the copy: %d err=%v", migrations, err)
	}
}
