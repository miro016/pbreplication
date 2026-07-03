package pbreplication

import (
	"database/sql"
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/pocketbase/pocketbase/core"
)

//go:embed dashboard
var dashboardFS embed.FS

// statusResponse is the JSON returned to the dashboard.
type statusResponse struct {
	NodeID       string           `json:"node_id"`
	NodeURL      string           `json:"node_url"`
	Bootstrapped bool             `json:"bootstrapped"`
	HLC          string           `json:"hlc"`
	SyncInterval string           `json:"sync_interval"`
	Members      []*memberStatus  `json:"members"`
	Vector       map[string]int64 `json:"vector"`
	OplogSize    int64            `json:"oplog_size"`
	PendingOps   int              `json:"pending_ops"`
	MissingBlobs int              `json:"missing_blobs"`
	Applied      int64            `json:"applied_total"`
	Failed       int64            `json:"failed_total"`
	Blocked      int64            `json:"blocked_total"`
	LastError    string           `json:"last_error,omitempty"`
}

type memberStatus struct {
	NodeID    string `json:"node_id"`
	URL       string `json:"url"`
	Reachable bool   `json:"reachable"`
	Healthy   bool   `json:"healthy"`
	Self      bool   `json:"self"`
	JoinedAt  string `json:"joined_at"`
	LastSeen  string `json:"last_seen"`
	Applied   int64  `json:"applied_seq"` // our vector entry for this node
	LastError string `json:"last_error,omitempty"`
}

func (r *Replicator) handleStatus(e *core.RequestEvent) error {
	db := r.app.DB()

	members, err := listMembers(db, false)
	if err != nil {
		return e.InternalServerError("failed to list members", nil)
	}
	vector, err := r.currentVector()
	if err != nil {
		return e.InternalServerError("failed to compute vector", nil)
	}

	var oplogSize sql.NullInt64
	_ = db.NewQuery(`SELECT COUNT(*) FROM _repl_oplog`).Row(&oplogSize)

	done, _ := getState(db, stateBootstrapDone)

	resp := &statusResponse{
		NodeID:       r.nodeID,
		NodeURL:      r.cfg.NodeURL,
		Bootstrapped: done != "",
		HLC:          r.clock.Current(),
		SyncInterval: r.cfg.SyncInterval.String(),
		Vector:       vector,
		OplogSize:    oplogSize.Int64,
		PendingOps:   r.pendingCount(),
		MissingBlobs: r.missingBlobCount(),
		Applied:      r.stats.applied.Load(),
		Failed:       r.stats.failed.Load(),
		Blocked:      r.stats.blocked.Load(),
	}
	if v := r.stats.lastError.Load(); v != nil {
		resp.LastError, _ = v.(string)
	}

	for _, m := range members {
		ms := &memberStatus{
			NodeID:    m.NodeID,
			URL:       m.URL,
			Reachable: m.Reachable,
			Healthy:   r.isHealthy(m),
			Self:      m.NodeID == r.nodeID,
			JoinedAt:  m.JoinedAt,
			LastSeen:  m.LastSeen,
			Applied:   vector[m.NodeID],
		}
		if v, ok := r.memberErrs.Load(m.NodeID); ok {
			ms.LastError, _ = v.(string)
		}
		if ov, ok := r.urlOverrides.Load(m.NodeID); ok {
			ms.URL = ov.(string) + " (override, advertised: " + m.URL + ")"
		}
		resp.Members = append(resp.Members, ms)
	}

	return e.JSON(http.StatusOK, resp)
}

// handleDashboard serves the embedded standalone dashboard page. The
// page itself is public; every data endpoint it calls requires a
// superuser token (read from the admin UI's localStorage or prompted).
func (r *Replicator) handleDashboard(e *core.RequestEvent) error {
	data, err := dashboardFS.ReadFile("dashboard/index.html")
	if err != nil {
		return e.NotFoundError("dashboard asset missing", nil)
	}
	return e.HTML(http.StatusOK, string(data))
}

// registerUIExtension hooks into PocketBase's experimental superuser UI
// extension API to add a "Replication" tab to the admin UI sidebar
// (opening the dashboard in an overlay, authenticated with the SPA's
// live superuser token). May break on PocketBase upgrades - the script
// degrades to a floating link, and the standalone dashboard page is the
// stable interface either way.
func (r *Replicator) registerUIExtension(se *core.ServeEvent) {
	sub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		return
	}
	se.UIExtensions = append(se.UIExtensions, core.UIExtension{
		Name: "pbreplication",
		FS:   sub,
	})

	// PocketBase serves /_/extensions.js from a fixed URL with a 14-day
	// Cache-Control (apis/extensions.go) — browsers would keep loading a
	// stale copy long after the extension script changed. PB only sets
	// that header when none is present yet, so pre-setting no-cache here
	// (our middleware runs before PB's priority-9999 extensions binder)
	// makes browsers revalidate on every admin UI load.
	se.Router.BindFunc(func(e *core.RequestEvent) error {
		if strings.HasSuffix(e.Request.URL.Path, "/_/extensions.js") {
			e.Response.Header().Set("Cache-Control", "no-cache")
		}
		return e.Next()
	})

	r.logInfo("admin UI extension registered (Replication tab in /_/)")
}
