package pbreplication

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
)

func TestDedicatedReplicationListener(t *testing.T) {
	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID:              "nodeA0000000001",
		NodeURL:             "http://nodeA.test:8091",
		ClusterSecret:       testSecret,
		ReplicationBindAddr: "127.0.0.1:0",
	})

	if err := r.startReplicationListener(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.stopReplicationListener)
	base := "http://" + r.replLn.Addr().String()

	// unauthenticated requests are rejected by the cluster HMAC
	resp, err := http.Get(base + "/api/replication/ping")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated ping: expected 401, got %d", resp.StatusCode)
	}

	// a signed request works end-to-end on the dedicated port
	req, _ := http.NewRequest(http.MethodGet, base+"/api/replication/ping", nil)
	r.signRequest(req, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed ping: expected 200, got %d", resp.StatusCode)
	}
	var pong struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pong); err != nil || pong.NodeID != r.nodeID {
		t.Fatalf("bad ping response: %+v err=%v", pong, err)
	}

	// operator endpoints must NOT exist on the replication listener
	req, _ = http.NewRequest(http.MethodGet, base+"/api/replication/status", nil)
	r.signRequest(req, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status on replication listener: expected 404, got %d", resp.StatusCode)
	}

	// idempotent start (OnServe may be re-entered in tests)
	if err := r.startReplicationListener(); err != nil {
		t.Fatal(err)
	}
}

// mainRouterFor builds PocketBase's router with the replicator's routes
// registered, as OnServe would.
func mainRouterFor(t *testing.T, app core.App, r *Replicator) http.Handler {
	t.Helper()
	pbRouter, err := apis.NewRouter(app)
	if err != nil {
		t.Fatal(err)
	}
	r.registerRoutes(&core.ServeEvent{App: app, Router: pbRouter})
	h, err := pbRouter.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestNodeRoutesLeaveMainRouterWhenDedicated(t *testing.T) {
	// default: node-to-node endpoints answer on the app router
	app, r := newTestNode(t, "nodeA0000000001")
	h := mainRouterFor(t, app, r)
	req := httptest.NewRequest(http.MethodGet, "/api/replication/ping", nil)
	r.signRequest(req, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ping on app router without dedicated listener: expected 200, got %d", rec.Code)
	}

	// with a dedicated listener configured they disappear from the app port
	app2 := newTestAppOnly(t)
	r2 := newTestNodeCfg(t, app2, Config{
		NodeID:              "nodeB0000000001",
		NodeURL:             "http://nodeB.test:8091",
		ClusterSecret:       testSecret,
		ReplicationBindAddr: "127.0.0.1:0",
	})
	h2 := mainRouterFor(t, app2, r2)
	req2 := httptest.NewRequest(http.MethodGet, "/api/replication/ping", nil)
	r2.signRequest(req2, nil)
	rec2 := httptest.NewRecorder()
	h2.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("ping on app router with dedicated listener: expected 404, got %d", rec2.Code)
	}

	// ...while the operator endpoints stay on the app port (dashboard
	// shell needs no auth)
	rec3 := httptest.NewRecorder()
	h2.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/api/replication/dashboard", nil))
	if rec3.Code != http.StatusOK {
		t.Fatalf("dashboard on app router: expected 200, got %d", rec3.Code)
	}
}
