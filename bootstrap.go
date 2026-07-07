package pbreplication

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// wire shapes ---------------------------------------------------------

type joinRequest struct {
	NodeID string `json:"node_id"`
	URL    string `json:"url,omitempty"`
}

type joinResponse struct {
	NodeID  string           `json:"node_id"`
	Members []*member        `json:"members"`
	Vector  map[string]int64 `json:"vector"`
	// URLVerified reports whether the seed could reach the URL the
	// joiner advertised (callback ping succeeded).
	URLVerified bool `json:"url_verified"`
}

type snapshotMeta struct {
	NodeID      string            `json:"node_id"`
	Collections []json.RawMessage `json:"collections"`
	Vector      map[string]int64  `json:"vector"`
	Members     []*member         `json:"members"`
	// HorizonHLC is the peer's tombstone GC cutoff: deletes older than
	// this may already be compacted away on the peer.
	HorizonHLC string `json:"horizon_hlc"`
	// AppliedMigrations lists the file names in the peer's _migrations
	// table so a joining node can skip migrations the cluster already
	// ran. Deliberately NOT omitempty: nil means "peer too old to
	// report", [] means "none applied".
	AppliedMigrations []string `json:"applied_migrations"`
}

type snapshotRecordItem struct {
	Payload json.RawMessage     `json:"payload"`
	HLC     string              `json:"hlc"`
	SrcNode string              `json:"src_node"`
	Files   map[string][]string `json:"files,omitempty"`
}

type snapshotRecordsPage struct {
	Items     []*snapshotRecordItem `json:"items"`
	NextAfter string                `json:"next_after"`
}

// ---------------------------------------------------------------------
// startup decision

// bootstrapOrRejoin runs once when the node starts serving.
func (r *Replicator) bootstrapOrRejoin() error {
	done, err := getState(r.app.DB(), stateBootstrapDone)
	if err != nil {
		return err
	}

	if r.cfg.SeedURL == "" {
		// first node of a new cluster (or a standalone restart)
		if done == "" {
			if err := setState(r.app.NonconcurrentDB(), stateBootstrapDone, nowStr()); err != nil {
				return err
			}
			r.logInfo("cluster initialized", "node", r.nodeID)
		}
		return nil
	}

	// announce ourselves (idempotent; also refreshes our URL after a
	// restart with a changed address)
	join, err := r.joinCluster()
	if err != nil {
		if done == "" {
			return fmt.Errorf("initial join via seed %s failed: %w", r.cfg.SeedURL, err)
		}
		r.logError("re-join announce failed (anti-entropy continues)", err)
		return nil
	}

	if done == "" {
		meta, err := r.snapshotFrom(r.cfg.SeedURL, false)
		if err != nil {
			return fmt.Errorf("initial snapshot failed: %w", err)
		}
		// Record which migrations the cluster already ran BEFORE
		// marking the bootstrap done: if this fails, a restart must
		// repeat defer+sync+import instead of running every deferred
		// migration against the already-synced data.
		if err := r.importClusterMigrations(meta.AppliedMigrations); err != nil {
			return fmt.Errorf("importing cluster migration history failed: %w", err)
		}
		if err := setState(r.app.NonconcurrentDB(), stateBootstrapDone, nowStr()); err != nil {
			return err
		}
		r.logInfo("initial bootstrap complete", "node", r.nodeID, "seed", r.cfg.SeedURL)
		// Now run only the migrations the cluster has NOT applied. A
		// failure here doesn't fail the bootstrap: on restart the defer
		// branch is skipped and PocketBase's serve-time migration run
		// retries exactly the unapplied ones.
		if err := r.runDeferredMigrations(); err != nil {
			r.logError("post-sync app migrations failed", err)
		}
	} else {
		r.logInfo("rejoined cluster", "node", r.nodeID, "members", len(join.Members))
	}

	wake(r.pullWake)
	return nil
}

// joinCluster registers this node with the seed and merges the member
// list it returns.
func (r *Replicator) joinCluster() (*joinResponse, error) {
	req := &joinRequest{NodeID: r.nodeID, URL: r.cfg.NodeURL}
	var resp joinResponse
	if err := r.callPeer(r.cfg.SeedURL, http.MethodPost, "/api/replication/join", req, &resp); err != nil {
		return nil, err
	}
	r.mergeMembers(resp.Members)

	if r.cfg.NodeURL != "" && !resp.URLVerified {
		r.logError("cluster peers cannot reach this node's advertised URL - "+
			"check NodeURL/PBR_NODE_URL (it must be reachable from the OTHER nodes); "+
			"replication continues in pull-only mode",
			fmt.Errorf("seed could not call back %s", r.cfg.NodeURL))
	}

	// The member list carries the URL the seed advertises about ITSELF,
	// which may only resolve inside the seed's own network (e.g. a
	// docker-internal "http://node1:8090" while we joined through a
	// public domain). If that advertised URL is not reachable from
	// here, keep talking to the seed through the URL that demonstrably
	// works.
	if resp.NodeID != "" && resp.NodeID != r.nodeID {
		advertised := ""
		for _, m := range resp.Members {
			if m.NodeID == resp.NodeID {
				advertised = m.URL
				break
			}
		}
		if advertised != "" && advertised != r.cfg.SeedURL && !r.verifyPeerURL(advertised, resp.NodeID) {
			r.urlOverrides.Store(resp.NodeID, r.cfg.SeedURL)
			r.logInfo("seed advertises a URL that is not reachable from this node - using the configured seed URL instead",
				"advertised", advertised, "using", r.cfg.SeedURL)
		}
	}

	return &resp, nil
}

// ---------------------------------------------------------------------
// snapshot resync trigger (used when a peer compacted past our cursor)

func (r *Replicator) triggerSnapshotResync(from *member) {
	if !r.resyncInFlight.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer r.resyncInFlight.Store(false)
		r.logInfo("peer compacted past our cursor - running snapshot resync", "peer", from.NodeID)
		if _, err := r.snapshotFrom(r.peerURL(from), true); err != nil {
			r.logError("snapshot resync failed", err)
		}
	}()
}

// ---------------------------------------------------------------------
// snapshot client

// snapshotFrom pulls a full snapshot (schema + records + files) from a
// peer and applies it through the regular LWW apply path, so newer
// local writes always survive. With reconcile=true it additionally
// deletes local records that no longer exist on the peer and whose
// last local write predates the peer's tombstone horizon (guards
// against resurrecting records whose tombstones were compacted away).
// The peer's snapshot meta is returned so callers can inspect it (e.g.
// the applied-migrations list during the initial bootstrap).
func (r *Replicator) snapshotFrom(baseURL string, reconcile bool) (*snapshotMeta, error) {
	var meta snapshotMeta
	if err := r.callPeer(baseURL, http.MethodGet, "/api/replication/snapshot/meta", nil, &meta); err != nil {
		return nil, err
	}
	r.mergeMembers(meta.Members)

	// 1. schema first
	for _, raw := range meta.Collections {
		var probe struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			continue
		}
		o := &op{
			SrcNode:  meta.NodeID,
			HLC:      encodeHLC(0, 1), // snapshot schema never beats real ops
			Type:     opColUpsert,
			ColID:    probe.ID,
			ColName:  probe.Name,
			RecordID: probe.ID,
			Payload:  raw,
		}
		if err := r.applyCollectionOp(o); err != nil {
			r.logError("snapshot: apply collection "+probe.Name, err)
		}
	}

	// 2. records, collection by collection
	cols, err := r.app.FindAllCollections()
	if err != nil {
		return nil, err
	}
	for _, col := range cols {
		if !r.isReplicated(col) {
			continue
		}
		seen, err := r.snapshotCollection(baseURL, meta.NodeID, col)
		if err != nil {
			// skip this collection (anti-entropy will still converge it);
			// crucially, do NOT reconcile against an incomplete seen-set
			r.logError("snapshot collection "+col.Name+" (skipped)", err)
			continue
		}
		if reconcile {
			r.reconcileCollection(col, seen, meta.HorizonHLC)
		}
	}

	// 3. adopt the peer's vector (captured BEFORE the record paging, so
	// anything written meanwhile is replayed afterwards - idempotent)
	db := r.app.NonconcurrentDB()
	for src, seq := range meta.Vector {
		if src == r.nodeID {
			continue
		}
		cur, _ := loadVectorEntry(db, src)
		if seq > cur {
			if err := setState(db, stateVectorPrefix+src, fmt.Sprintf("%d", seq)); err != nil {
				return nil, err
			}
		}
	}

	return &meta, nil
}

// snapshotCollection pages all records of one collection from the peer
// and applies each through the LWW apply path. Returns the set of
// record ids present on the peer.
func (r *Replicator) snapshotCollection(baseURL, peerNode string, col *core.Collection) (map[string]bool, error) {
	seen := map[string]bool{}
	after := ""

	for {
		path := fmt.Sprintf("/api/replication/snapshot/records?collection=%s&after=%s&limit=%d",
			url.QueryEscape(col.Name), url.QueryEscape(after), r.cfg.MaxBatch)

		var page snapshotRecordsPage
		if err := r.callPeer(baseURL, http.MethodGet, path, nil, &page); err != nil {
			return seen, err
		}

		for _, item := range page.Items {
			var probe struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(item.Payload, &probe); err != nil || probe.ID == "" {
				continue
			}
			seen[probe.ID] = true

			src := item.SrcNode
			if src == "" {
				src = peerNode
			}
			hlcStr := item.HLC
			if hlcStr == "" {
				hlcStr = encodeHLC(0, 1)
			}
			o := &op{
				SrcNode:  src,
				HLC:      hlcStr,
				Type:     opUpsert,
				ColID:    col.Id,
				ColName:  col.Name,
				RecordID: probe.ID,
				Payload:  item.Payload,
				Files:    item.Files,
			}
			if o.SrcNode == r.nodeID {
				continue // our own record state; we already have it
			}
			if err := r.applyRecordOp(o); err != nil {
				r.stats.failed.Add(1)
				r.logError("snapshot: apply record "+col.Name+"/"+probe.ID, err)
			}
		}

		if page.NextAfter == "" || len(page.Items) == 0 {
			return seen, nil
		}
		after = page.NextAfter
	}
}

// reconcileCollection deletes local records that don't exist on the
// snapshot source anymore, unless they were written after the peer's
// tombstone horizon (in which case they are legitimately newer local
// writes that will replicate out normally).
func (r *Replicator) reconcileCollection(col *core.Collection, seen map[string]bool, horizonHLC string) {
	var ids []string
	err := r.app.DB().NewQuery(fmt.Sprintf("SELECT id FROM {{%s}}", col.Name)).Column(&ids)
	if err != nil {
		r.logError("reconcile: list local records "+col.Name, err)
		return
	}

	for _, id := range ids {
		if seen[id] {
			continue
		}
		ver, err := getVersion(r.app.DB(), col.Id, id)
		if err != nil {
			continue
		}
		// keep records written after the horizon - they're newer local
		// writes, not resurrections
		if ver != nil && horizonHLC != "" && ver.HLC >= horizonHLC {
			continue
		}

		o := &op{
			SrcNode:  "snapshot-reconcile",
			HLC:      r.clock.Now(),
			Type:     opDelete,
			ColID:    col.Id,
			ColName:  col.Name,
			RecordID: id,
		}
		if err := r.applyReconcileDelete(col, o); err != nil {
			r.logError("reconcile: delete "+col.Name+"/"+id, err)
		}
	}
}

// applyReconcileDelete removes a resurrected record locally without
// replicating the deletion (every healthy node either already deleted
// it or will reconcile the same way).
func (r *Replicator) applyReconcileDelete(col *core.Collection, o *op) error {
	ctx := markedCtx(context.Background(), o)
	return r.app.RunInTransaction(func(txApp core.App) error {
		rec, err := txApp.FindRecordById(col, o.RecordID)
		if err != nil || rec == nil {
			return nil
		}
		if err := txApp.DeleteWithContext(ctx, rec); err != nil {
			return err
		}
		return upsertVersion(txApp.NonconcurrentDB(), col.Id, o.RecordID, o.HLC, o.SrcNode, true)
	})
}

// ---------------------------------------------------------------------
// snapshot server side

// serveSnapshotMeta returns everything a joining node needs before
// paging records.
func (r *Replicator) serveSnapshotMeta(e *core.RequestEvent) error {
	cols, err := r.app.FindAllCollections()
	if err != nil {
		return e.InternalServerError("failed to list collections", nil)
	}

	raws := make([]json.RawMessage, 0, len(cols))
	for _, col := range cols {
		b, err := exportCollectionJSON(col)
		if err != nil {
			continue
		}
		raws = append(raws, b)
	}

	vector, err := r.currentVector()
	if err != nil {
		return e.InternalServerError("failed to compute vector", nil)
	}
	members, _ := listMembers(r.app.DB(), false)

	applied, err := listAppliedMigrations(r.app.DB())
	if err != nil {
		return e.InternalServerError("failed to list applied migrations", nil)
	}

	horizon := encodeHLC(uint64(time.Now().Add(-r.cfg.TombstoneRetention).UnixMilli()), 0)

	return e.JSON(http.StatusOK, &snapshotMeta{
		NodeID:            r.nodeID,
		Collections:       raws,
		Vector:            vector,
		Members:           members,
		HorizonHLC:        horizon,
		AppliedMigrations: applied,
	})
}

// serveSnapshotRecords pages one collection's records ordered by id.
func (r *Replicator) serveSnapshotRecords(e *core.RequestEvent) error {
	colName := e.Request.URL.Query().Get("collection")
	after := e.Request.URL.Query().Get("after")
	limit := r.cfg.MaxBatch
	if v := e.Request.URL.Query().Get("limit"); v != "" {
		fmt.Sscanf(v, "%d", &limit)
		if limit <= 0 || limit > r.cfg.MaxBatch {
			limit = r.cfg.MaxBatch
		}
	}

	col, err := r.app.FindCachedCollectionByNameOrId(colName)
	if err != nil || col == nil || !r.isReplicated(col) {
		// unknown/excluded collections yield an empty page (not an
		// error) so a bootstrap against a peer with a divergent schema
		// can still complete
		return e.JSON(http.StatusOK, &snapshotRecordsPage{})
	}

	records := []*core.Record{}
	q := r.app.RecordQuery(col).OrderBy("id ASC").Limit(int64(limit))
	if after != "" {
		q.AndWhere(dbx.NewExp("id > {:after}", dbx.Params{"after": after}))
	}
	if err := q.All(&records); err != nil {
		return e.InternalServerError("failed to query records", nil)
	}

	page := &snapshotRecordsPage{Items: make([]*snapshotRecordItem, 0, len(records))}
	for _, rec := range records {
		data, err := rec.DBExport(r.app)
		if err != nil {
			continue
		}
		payload, err := json.Marshal(data)
		if err != nil {
			continue
		}

		item := &snapshotRecordItem{Payload: payload}

		if ver, _ := getVersion(r.app.DB(), col.Id, rec.Id); ver != nil {
			item.HLC = ver.HLC
			item.SrcNode = ver.SrcNode
		}

		for _, f := range col.Fields {
			if f.Type() != core.FieldTypeFile {
				continue
			}
			names := rec.GetStringSlice(f.GetName())
			if len(names) == 0 {
				continue
			}
			if item.Files == nil {
				item.Files = map[string][]string{}
			}
			item.Files[f.GetName()] = names
		}

		page.Items = append(page.Items, item)
		page.NextAfter = rec.Id
	}

	if len(records) < limit {
		page.NextAfter = ""
	}

	return e.JSON(http.StatusOK, page)
}
