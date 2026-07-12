package pbreplication

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

func fakeMigrationsPeer(t *testing.T, nodeID string, applied []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/replication/migrations" {
			http.NotFound(w, req)
			return
		}
		json.NewEncoder(w).Encode(&migrationsResponse{NodeID: nodeID, Applied: applied})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCoordinateMigrationsUnion(t *testing.T) {
	stashAppMigrations(t)
	app := newTestAppOnly(t)

	ran := map[string]bool{}
	for _, f := range []string{"m1.go", "m2.go", "m3.go"} {
		file := f
		core.AppMigrations.Register(func(txApp core.App) error {
			ran[file] = true
			return nil
		}, nil, file)
	}

	r := newTestNodeCfg(t, app, Config{
		NodeID:        "nodeA0000000001", // lowest id -> leader, no jitter wait
		SeedURL:       "http://seed.test:8090",
		ClusterSecret: testSecret,
	})
	if !r.migrationsDeferred {
		t.Fatal("precondition: migrations must be deferred")
	}

	// two peers with DIFFERENT applied sets; the union covers m1+m2
	p1 := fakeMigrationsPeer(t, "nodeX0000000001", []string{"m1.go"})
	p2 := fakeMigrationsPeer(t, "nodeY0000000001", []string{"m2.go"})
	for i, srv := range []*httptest.Server{p1, p2} {
		id := []string{"nodeX0000000001", "nodeY0000000001"}[i]
		if err := upsertMember(app.NonconcurrentDB(), &member{
			NodeID: id, URL: srv.URL, Reachable: true, LastSeen: nowStr(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	r.coordinateMigrations()

	if ran["m1.go"] || ran["m2.go"] {
		t.Fatalf("peer-applied migrations re-ran locally: %v", ran)
	}
	if !ran["m3.go"] {
		t.Fatal("migration no peer ran must execute locally")
	}
	// all three must now be recorded as applied
	for _, f := range []string{"m1.go", "m2.go", "m3.go"} {
		if !migrationApplied(t, app, f) {
			t.Fatalf("%s missing from _migrations", f)
		}
	}
	if r.migrationsDeferred {
		t.Fatal("deferred flag must clear after the run")
	}
}

func TestCoordinateMigrationsFallbackWhenUnreachable(t *testing.T) {
	stashAppMigrations(t)
	app := newTestAppOnly(t)

	ran := false
	core.AppMigrations.Register(func(txApp core.App) error {
		ran = true
		return nil
	}, nil, "solo.go")

	r := newTestNodeCfg(t, app, Config{
		NodeID:                       "nodeA0000000001",
		SeedURL:                      "http://seed.test:8090",
		ClusterSecret:                testSecret,
		MigrationCoordinationTimeout: 500 * time.Millisecond,
	})

	// an unreachable peer (closed port answers immediately)
	if err := upsertMember(app.NonconcurrentDB(), &member{
		NodeID: "nodeZ0000000001", URL: "http://127.0.0.1:1", LastSeen: nowStr(),
	}); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	r.coordinateMigrations()
	if time.Since(start) > 10*time.Second {
		t.Fatal("fallback took too long")
	}

	if !ran {
		t.Fatal("with zero reachable peers the deferred migration must run locally")
	}
	if !migrationApplied(t, app, "solo.go") {
		t.Fatal("solo.go missing from _migrations")
	}
}

func TestMigrationsEndpointShape(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	_ = app

	req := httptest.NewRequest(http.MethodGet, "http://n/api/replication/migrations", nil)
	rec := httptest.NewRecorder()
	e := &core.RequestEvent{}
	e.Response = rec
	e.Request = req
	if err := r.handleMigrations(e); err != nil {
		t.Fatal(err)
	}

	var resp migrationsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != r.nodeID {
		t.Fatalf("node_id = %q", resp.NodeID)
	}
	if resp.Applied == nil {
		t.Fatal("applied must never be nil on the wire (nil means 'too old to report')")
	}
	if len(resp.Applied) == 0 {
		t.Fatal("the test app's own migrations must be listed")
	}
}
