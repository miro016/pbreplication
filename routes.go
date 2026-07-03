package pbreplication

import (
	"net/http"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
)

// registerRoutes mounts all replication endpoints on PocketBase's own
// HTTP router (no extra port needed).
func (r *Replicator) registerRoutes(se *core.ServeEvent) {
	g := se.Router.Group("/api/replication")

	// --- node-to-node endpoints (cluster secret HMAC) ---
	n := g.Group("")
	n.BindFunc(r.requireClusterAuth)
	n.POST("/join", r.handleJoin)
	n.GET("/ping", r.handlePing)
	n.POST("/ops", r.handleOps)
	n.POST("/pull", r.handlePull)
	n.GET("/file/{collection}/{recordId}/{filename}", r.handleFile)
	n.GET("/snapshot/meta", r.serveSnapshotMeta)
	n.GET("/snapshot/records", r.serveSnapshotRecords)

	// --- admin endpoints ---
	g.GET("/status", r.handleStatus).Bind(apis.RequireSuperuserAuth())
	g.GET("/firewall/summary", r.handleFirewallSummary).Bind(apis.RequireSuperuserAuth())
	g.GET("/clients", r.handleClients).Bind(apis.RequireSuperuserAuth())
	g.GET("/world.json", r.handleWorldMap).Bind(apis.RequireSuperuserAuth())
	g.GET("/dashboard", r.handleDashboard) // HTML shell; its data calls require superuser auth
}

// handlePing answers reachability callbacks.
func (r *Replicator) handlePing(e *core.RequestEvent) error {
	return e.JSON(http.StatusOK, map[string]string{"node_id": r.nodeID})
}

// handleJoin registers a (re)joining node and hands it the current
// member list. If the joiner advertises a URL, it is verified with a
// callback ping before being gossiped as reachable.
func (r *Replicator) handleJoin(e *core.RequestEvent) error {
	req := &joinRequest{}
	if err := e.BindBody(req); err != nil || req.NodeID == "" {
		return e.BadRequestError("invalid join request", nil)
	}

	reachable := false
	if req.URL != "" {
		reachable = r.verifyPeerURL(req.URL, req.NodeID)
	}

	if req.NodeID != r.nodeID {
		if err := upsertMember(r.app.NonconcurrentDB(), &member{
			NodeID:    req.NodeID,
			URL:       req.URL,
			Reachable: reachable,
			LastSeen:  nowStr(),
		}); err != nil {
			return e.InternalServerError("failed to register member", nil)
		}
	}

	members, err := listMembers(r.app.DB(), false)
	if err != nil {
		return e.InternalServerError("failed to list members", nil)
	}
	vector, err := r.currentVector()
	if err != nil {
		return e.InternalServerError("failed to compute vector", nil)
	}

	r.logInfo("node joined", "node", req.NodeID, "url", req.URL, "reachable", reachable)

	return e.JSON(http.StatusOK, &joinResponse{
		NodeID:      r.nodeID,
		Members:     members,
		Vector:      vector,
		URLVerified: reachable,
	})
}

// verifyPeerURL confirms that the advertised URL actually answers as
// the claimed node (guards against typos and unreachable NAT'd nodes).
func (r *Replicator) verifyPeerURL(url, expectedNodeID string) bool {
	var resp struct {
		NodeID string `json:"node_id"`
	}
	if err := r.callPeer(url, http.MethodGet, "/api/replication/ping", nil, &resp); err != nil {
		return false
	}
	return resp.NodeID == expectedNodeID
}

// handleOps receives a push batch.
func (r *Replicator) handleOps(e *core.RequestEvent) error {
	req := &pushRequest{}
	if err := e.BindBody(req); err != nil {
		return e.BadRequestError("invalid ops payload", nil)
	}

	r.noteSender(req.Sender)
	r.mergeMembers(req.Members)

	if err := r.ingestOps(req.Ops); err != nil {
		return e.InternalServerError("failed to ingest ops", nil)
	}

	vector, err := r.currentVector()
	if err != nil {
		return e.InternalServerError("failed to compute vector", nil)
	}
	return e.JSON(http.StatusOK, &pushResponse{NodeID: r.nodeID, Vector: vector})
}

// handlePull serves an anti-entropy request: every op the caller's
// vector doesn't cover yet.
func (r *Replicator) handlePull(e *core.RequestEvent) error {
	req := &pullRequest{}
	if err := e.BindBody(req); err != nil {
		return e.BadRequestError("invalid pull payload", nil)
	}
	if req.Limit <= 0 || req.Limit > r.cfg.MaxBatch {
		req.Limit = r.cfg.MaxBatch
	}

	r.noteSender(req.Sender)

	ops, snapshotRequired, err := opsAfterVector(r.app.DB(), req.Vector, req.Limit)
	if err != nil {
		return e.InternalServerError("failed to read oplog", nil)
	}

	vector, err := r.currentVector()
	if err != nil {
		return e.InternalServerError("failed to compute vector", nil)
	}
	members, _ := listMembers(r.app.DB(), false)

	return e.JSON(http.StatusOK, &pullResponse{
		NodeID:           r.nodeID,
		Ops:              ops,
		Vector:           vector,
		Members:          members,
		SnapshotRequired: snapshotRequired,
	})
}

// noteSender keeps membership fresh from authenticated exchanges (this
// is also how pull-only nodes without a URL stay visible/healthy).
func (r *Replicator) noteSender(s senderInfo) {
	if s.NodeID == "" || s.NodeID == r.nodeID {
		return
	}
	db := r.app.NonconcurrentDB()
	cur, err := getMember(db, s.NodeID)
	if err != nil {
		return
	}
	if cur == nil {
		_ = upsertMember(db, &member{
			NodeID:    s.NodeID,
			URL:       s.URL,
			Reachable: s.URL != "",
			LastSeen:  nowStr(),
		})
		return
	}
	cur.LastSeen = nowStr()
	cur.Removed = false
	if s.URL != "" && s.URL != cur.URL {
		cur.URL = s.URL
		cur.Reachable = true
	}
	_ = upsertMember(db, cur)
}

// handleFile streams a stored record file to a peer.
func (r *Replicator) handleFile(e *core.RequestEvent) error {
	return r.serveBlob(e,
		e.Request.PathValue("collection"),
		e.Request.PathValue("recordId"),
		e.Request.PathValue("filename"),
	)
}
