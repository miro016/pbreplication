package pbreplication

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/tools/router"
)

func strictIdentityBody(t *testing.T, nodeID, instanceID string, fresh bool) string {
	return strictIdentityBodyWithURL(t, nodeID, instanceID, "", fresh)
}

func strictIdentityBodyWithURL(t *testing.T, nodeID, instanceID, url string, fresh bool) string {
	t.Helper()
	b, err := json.Marshal(&identityCheckRequest{
		NodeID:     nodeID,
		InstanceID: instanceID,
		URL:        url,
		Fresh:      fresh,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func requireIdentityStatus(t *testing.T, err error, status int) {
	t.Helper()
	var apiErr *router.ApiError
	if !errors.As(err, &apiErr) || apiErr.Status != status {
		t.Fatalf("err = %v, want %d ApiError", err, status)
	}
}

func TestHandleIdentityCheck(t *testing.T) {
	t.Run("seed owns configured id", func(t *testing.T) {
		_, r := newTestNode(t, "nodeA0000000001")
		_, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, r.NodeID(), "different-process", true))
		requireIdentityStatus(t, err, http.StatusConflict)
	})

	t.Run("unused id is available", func(t *testing.T) {
		_, r := newTestNode(t, "nodeA0000000001")
		rec, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, "unused-node", "joining-process", true))
		if err != nil || rec.Code != http.StatusOK {
			t.Fatalf("unused identity rejected: code=%d err=%v", rec.Code, err)
		}
	})

	t.Run("fresh node cannot reuse offline registered id", func(t *testing.T) {
		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "registered-node", LastSeen: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		_, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, "registered-node", "joining-process", true))
		requireIdentityStatus(t, err, http.StatusConflict)
	})

	t.Run("stopped member can restart with persisted id", func(t *testing.T) {
		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "restarting-node", LastSeen: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		rec, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, "restarting-node", "new-process", false))
		if err != nil || rec.Code != http.StatusOK {
			t.Fatalf("stopped member restart rejected: code=%d err=%v", rec.Code, err)
		}
	})

	t.Run("active pull-only member owns id", func(t *testing.T) {
		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "active-node", LastSeen: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		_, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, "active-node", "different-process", false))
		requireIdentityStatus(t, err, http.StatusConflict)
	})

	t.Run("callback distinguishes active process", func(t *testing.T) {
		owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(&identityPing{NodeID: "callback-node", InstanceID: "owner-process"})
		}))
		defer owner.Close()

		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "callback-node", URL: owner.URL,
			LastSeen: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		_, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBody(t, "callback-node", "joining-process", false))
		requireIdentityStatus(t, err, http.StatusConflict)
	})

	t.Run("same address may restart immediately after listener stops", func(t *testing.T) {
		stopped := httptest.NewServer(http.NotFoundHandler())
		stoppedURL := stopped.URL
		stopped.Close()

		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "restarting-node", URL: stoppedURL,
			LastSeen: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		rec, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBodyWithURL(t, "restarting-node", "new-process", stoppedURL, false))
		if err != nil || rec.Code != http.StatusOK {
			t.Fatalf("immediate restart at the same URL rejected: code=%d err=%v", rec.Code, err)
		}
	})

	t.Run("verified new address may replace recently active old address", func(t *testing.T) {
		stopped := httptest.NewServer(http.NotFoundHandler())
		stoppedURL := stopped.URL
		stopped.Close()
		joining := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(&identityPing{NodeID: "restarting-node", InstanceID: "new-process"})
		}))
		defer joining.Close()

		app, r := newTestNode(t, "seed-node")
		if err := upsertMember(app.DB(), &member{
			NodeID: "restarting-node", URL: stoppedURL,
			LastSeen: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
		rec, err := execHandler(t, r, r.handleIdentityCheck, http.MethodPost, identityCheckPath,
			strictIdentityBodyWithURL(t, "restarting-node", "new-process", joining.URL, false))
		if err != nil || rec.Code != http.StatusOK {
			t.Fatalf("restart at verified new address rejected: code=%d err=%v", rec.Code, err)
		}
	})
}

func TestCheckStrictNodeID(t *testing.T) {
	t.Run("fresh pre-bootstrap request uses configured id", func(t *testing.T) {
		var authorization string
		seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			authorization = req.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"available":true}`))
		}))
		defer seed.Close()

		app := newTestAppOnly(t)
		r, err := Register(app, Config{
			NodeID: "configured-node", StrictNodeID: true,
			SeedURL: seed.URL, ClusterSecret: testSecret,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.checkStrictNodeID(true); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(authorization, authScheme+" configured-node.") {
			t.Fatalf("pre-bootstrap auth did not use configured id: %q", authorization)
		}
	})

	t.Run("conflict is permanent duplicate error", func(t *testing.T) {
		seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			http.Error(w, `{"message":"node id is already taken"}`, http.StatusConflict)
		}))
		defer seed.Close()

		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID: "configured-node", StrictNodeID: true,
			SeedURL: seed.URL, ClusterSecret: testSecret,
		})
		err := r.checkStrictNodeID(false)
		if !errors.Is(err, errDuplicateNodeID) || !strings.Contains(err.Error(), "configured-node") {
			t.Fatalf("err = %v, want named duplicate-node error", err)
		}
	})

	t.Run("old seed blocks startup with upgrade message", func(t *testing.T) {
		seed := httptest.NewServer(http.NotFoundHandler())
		defer seed.Close()

		app := newTestAppOnly(t)
		r := newTestNodeCfg(t, app, Config{
			NodeID: "configured-node", StrictNodeID: true,
			SeedURL: seed.URL, ClusterSecret: testSecret,
		})
		err := r.checkStrictNodeID(false)
		if err == nil || !strings.Contains(err.Error(), "upgrade the seed") {
			t.Fatalf("err = %v, want seed upgrade guidance", err)
		}
	})
}

func TestStrictNodeIDRequiresConfiguredID(t *testing.T) {
	app := newTestAppOnly(t)
	_, err := Register(app, Config{StrictNodeID: true, ClusterSecret: testSecret})
	if err == nil || !strings.Contains(err.Error(), "StrictNodeID requires NodeID") {
		t.Fatalf("err = %v, want StrictNodeID validation failure", err)
	}
}

func TestStrictNodeIDRejectsFreshCopyBeforeSnapshot(t *testing.T) {
	identityChecks := 0
	snapshotRequests := 0
	seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case identityCheckPath:
			identityChecks++
			http.Error(w, `{"message":"node id is already taken"}`, http.StatusConflict)
		case "/api/replication/snapshot/db":
			snapshotRequests++
			http.Error(w, "must not be called", http.StatusInternalServerError)
		default:
			http.NotFound(w, req)
		}
	}))
	defer seed.Close()

	app := newTestAppOnly(t)
	r, err := Register(app, Config{
		NodeID: "taken-node", StrictNodeID: true,
		SeedURL: seed.URL, ClusterSecret: testSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	fresh := &dataDirApp{App: app, dir: t.TempDir()}
	err = r.maybeFullCopyBootstrap(fresh)
	if !errors.Is(err, errDuplicateNodeID) {
		t.Fatalf("err = %v, want duplicate-node startup failure", err)
	}
	if identityChecks != 1 || snapshotRequests != 0 {
		t.Fatalf("identity checks=%d snapshot requests=%d, want 1 and 0", identityChecks, snapshotRequests)
	}
}

func TestStrictJoinConflictDoesNotScheduleIdentityRegeneration(t *testing.T) {
	seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, `{"message":"node id is already taken"}`, http.StatusConflict)
	}))
	defer seed.Close()

	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID: "configured-node", StrictNodeID: true,
		SeedURL: seed.URL, ClusterSecret: testSecret,
	})
	if _, err := r.joinCluster(); !errors.Is(err, errDuplicateNodeID) {
		t.Fatalf("join error = %v, want duplicate node id", err)
	}
	if pending, err := getState(app.DB(), stateDupNodePending); err != nil || pending != "" {
		t.Fatalf("strict join scheduled identity regeneration: pending=%q err=%v", pending, err)
	}
}

func TestStrictJoinDuplicateResponseDoesNotScheduleIdentityRegeneration(t *testing.T) {
	seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(&joinResponse{
			NodeID: "configured-node", InstanceID: "different-process",
			Vector: map[string]int64{"configured-node": 42},
		})
	}))
	defer seed.Close()

	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID: "configured-node", StrictNodeID: true,
		SeedURL: seed.URL, ClusterSecret: testSecret,
	})
	if _, err := r.joinCluster(); !errors.Is(err, errDuplicateNodeID) {
		t.Fatalf("join error = %v, want duplicate node id", err)
	}
	if pending, err := getState(app.DB(), stateDupNodePending); err != nil || pending != "" {
		t.Fatalf("strict join response scheduled identity regeneration: pending=%q err=%v", pending, err)
	}
}

func TestClearDuplicateNodeIDFlag(t *testing.T) {
	app, r := newTestNode(t, "configured-node")
	if err := setState(app.DB(), stateDupNodePending, "-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.clearDuplicateNodeIDFlag(); err != nil {
		t.Fatal(err)
	}
	if pending, err := getState(app.DB(), stateDupNodePending); err != nil || pending != "" {
		t.Fatalf("pending flag = %q, err=%v; want empty", pending, err)
	}
}
