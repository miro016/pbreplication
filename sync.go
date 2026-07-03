package pbreplication

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pocketbase/dbx"
)

// wire shapes ---------------------------------------------------------

type senderInfo struct {
	NodeID string `json:"node_id"`
	URL    string `json:"url"`
}

type pushRequest struct {
	Sender  senderInfo `json:"sender"`
	Ops     []*op      `json:"ops"`
	Members []*member  `json:"members,omitempty"`
}

type pushResponse struct {
	NodeID string           `json:"node_id"`
	Vector map[string]int64 `json:"vector"`
}

type pullRequest struct {
	Sender senderInfo       `json:"sender"`
	Vector map[string]int64 `json:"vector"`
	Limit  int              `json:"limit"`
}

type pullResponse struct {
	NodeID           string           `json:"node_id"`
	Ops              []*op            `json:"ops"`
	Vector           map[string]int64 `json:"vector"`
	Members          []*member        `json:"members,omitempty"`
	SnapshotRequired bool             `json:"snapshot_required"`
}

func (r *Replicator) senderInfo() senderInfo {
	return senderInfo{NodeID: r.nodeID, URL: r.cfg.NodeURL}
}

// currentVector returns the node's contiguous per-source vector,
// including its own local sequence.
func (r *Replicator) currentVector() (map[string]int64, error) {
	db := r.app.DB()
	vec, err := loadVector(db)
	if err != nil {
		return nil, err
	}
	localSeq, err := getState(db, stateLocalSeq)
	if err != nil {
		return nil, err
	}
	var seq int64
	if localSeq != "" {
		fmt.Sscanf(localSeq, "%d", &seq)
	}
	if seq > vec[r.nodeID] {
		vec[r.nodeID] = seq
	}
	return vec, nil
}

// ---------------------------------------------------------------------
// ingest (shared by the push handler and the pull client)

// ingestOps stores remote ops in the local oplog (making this node a
// gossip relay for them), queues them for application and advances the
// contiguous vector.
func (r *Replicator) ingestOps(ops []*op) error {
	if len(ops) == 0 {
		return nil
	}

	db := r.app.NonconcurrentDB()
	touched := map[string]bool{}

	for _, o := range ops {
		if o.SrcNode == "" || o.SrcSeq <= 0 || o.SrcNode == r.nodeID {
			continue
		}
		if err := insertOp(db, o); err != nil {
			return err
		}
		touched[o.SrcNode] = true
		r.enqueueApply(o)
	}

	gap := false
	for src := range touched {
		cur, err := loadVectorEntry(db, src)
		if err != nil {
			return err
		}
		next, err := advanceVector(db, src, cur)
		if err != nil {
			return err
		}
		// a hole in the sequence means we missed earlier ops -> pull now
		var maxSeq int64
		if err := db.NewQuery(`SELECT COALESCE(MAX(src_seq), 0) FROM _repl_oplog WHERE src_node = {:s}`).
			Bind(dbx.Params{"s": src}).Row(&maxSeq); err == nil && maxSeq > next {
			gap = true
		}
	}

	if gap {
		wake(r.pullWake)
	}
	wake(r.pushWake) // relay to peers (gossip)
	return nil
}

// loadVectorEntry reads a single persisted vector entry.
func loadVectorEntry(db dbx.Builder, src string) (int64, error) {
	v, err := getState(db, stateVectorPrefix+src)
	if err != nil {
		return 0, err
	}
	var seq int64
	if v != "" {
		fmt.Sscanf(v, "%d", &seq)
	}
	return seq, nil
}

// ---------------------------------------------------------------------
// pusher

// pushLoop debounces local write signals and pushes fresh oplog entries
// to every reachable member.
func (r *Replicator) pushLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.pushWake:
		}

		// debounce: batch rapid successive writes
		timer := time.NewTimer(r.cfg.DebounceWindow)
	drain:
		for {
			select {
			case <-r.stopCh:
				timer.Stop()
				return
			case <-r.pushWake:
				// keep draining until the window elapses
			case <-timer.C:
				break drain
			}
		}

		r.pushRound()
	}
}

func (r *Replicator) pushRound() {
	peers := r.pushTargets()
	if len(peers) == 0 {
		return
	}

	members, _ := listMembers(r.app.DB(), false)

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(p *member) {
			defer wg.Done()
			r.pushToPeer(p, members)
		}(p)
	}
	wg.Wait()
}

func (r *Replicator) pushTargets() []*member {
	all, err := listMembers(r.app.DB(), false)
	if err != nil {
		return nil
	}
	targets := make([]*member, 0, len(all))
	for _, m := range all {
		// reachability is asymmetric (the flag reflects the seed's view),
		// so try any member with a URL - failures are cheap, visible via
		// notePeerErr, and healed by anti-entropy
		if m.NodeID != r.nodeID && m.URL != "" {
			targets = append(targets, m)
		}
	}
	return targets
}

func (r *Replicator) pushToPeer(p *member, memberList []*member) {
	r.cursorMu.Lock()
	cursor, ok := r.pushCursors[p.NodeID]
	r.cursorMu.Unlock()
	if !ok {
		cursor = r.initialPushCursor()
	}

	for {
		ops, last, err := opsAfterRowID(r.app.DB(), cursor, r.cfg.MaxBatch)
		if err != nil {
			r.logError("push: read oplog", err)
			return
		}
		if len(ops) == 0 {
			break
		}

		req := &pushRequest{Sender: r.senderInfo(), Ops: ops, Members: memberList}
		var resp pushResponse
		if err := r.callPeer(r.peerURL(p), http.MethodPost, "/api/replication/ops", req, &resp); err != nil {
			r.notePeerErr(p.NodeID, err)
			return // anti-entropy will heal
		}
		r.clearPeerErr(p.NodeID)

		cursor = last
		r.cursorMu.Lock()
		r.pushCursors[p.NodeID] = cursor
		r.cursorMu.Unlock()
		_ = touchMember(r.app.NonconcurrentDB(), p.NodeID)

		if len(ops) < r.cfg.MaxBatch {
			break
		}
	}
}

// initialPushCursor is the rowid from which pushes start for a peer we
// haven't pushed to in this process lifetime. History is the job of
// anti-entropy pulls, so start at the process-start oplog head.
func (r *Replicator) initialPushCursor() int64 {
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()
	return r.startRowID
}

// ---------------------------------------------------------------------
// anti-entropy

// antiEntropyLoop periodically pulls missing ops from every member.
func (r *Replicator) antiEntropyLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.cfg.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
		case <-r.pullWake:
		}
		r.syncRound()
	}
}

func (r *Replicator) syncRound() {
	members, err := listMembers(r.app.DB(), false)
	if err != nil {
		return
	}

	for _, m := range members {
		if m.NodeID == r.nodeID || m.URL == "" {
			continue
		}
		if err := r.pullFromPeer(m); err != nil {
			r.notePeerErr(m.NodeID, err)
			continue // unreachable peers are expected
		}
		r.clearPeerErr(m.NodeID)
	}

	r.retryPending()
	r.retryMissingBlobs()
	r.flushClients()

	// periodically persist the clock so restarts resume monotonically
	_ = setState(r.app.NonconcurrentDB(), stateHLC, r.clock.Current())
}

func (r *Replicator) pullFromPeer(m *member) error {
	for {
		vector, err := r.currentVector()
		if err != nil {
			return err
		}

		req := &pullRequest{Sender: r.senderInfo(), Vector: vector, Limit: r.cfg.MaxBatch}
		var resp pullResponse
		if err := r.callPeer(r.peerURL(m), http.MethodPost, "/api/replication/pull", req, &resp); err != nil {
			return err
		}

		_ = touchMember(r.app.NonconcurrentDB(), m.NodeID)
		r.mergeMembers(resp.Members)

		if resp.SnapshotRequired {
			r.triggerSnapshotResync(m)
			return nil
		}

		if err := r.ingestOps(resp.Ops); err != nil {
			return err
		}

		if len(resp.Ops) < r.cfg.MaxBatch {
			// Complete pull: adopt the peer's vector. Safe because the
			// peer's vector covers exactly the effects contained in the
			// ops it retains (superseded ops it compacted away are, by
			// definition, covered by newer ops we just ingested). This
			// also lets the vector move past holes left by compaction.
			r.adoptVector(resp.Vector)
			return nil
		}
	}
}

// adoptVector raises local vector entries to the given values.
func (r *Replicator) adoptVector(vec map[string]int64) {
	db := r.app.NonconcurrentDB()
	for src, seq := range vec {
		if src == r.nodeID {
			continue
		}
		cur, err := loadVectorEntry(db, src)
		if err != nil || seq <= cur {
			continue
		}
		_ = setState(db, stateVectorPrefix+src, fmt.Sprintf("%d", seq))
	}
}

// ---------------------------------------------------------------------
// membership merge (autodiscovery)

// mergeMembers folds a member list received from a peer into the local
// table. Newest last_seen wins; on ties a removal flag wins.
func (r *Replicator) mergeMembers(list []*member) {
	if len(list) == 0 {
		return
	}
	db := r.app.NonconcurrentDB()

	for _, m := range list {
		if m.NodeID == "" || m.NodeID == r.nodeID {
			continue
		}
		cur, err := getMember(db, m.NodeID)
		if err != nil {
			continue
		}
		if cur == nil {
			_ = upsertMember(db, m)
			continue
		}
		if m.LastSeen > cur.LastSeen ||
			(m.LastSeen == cur.LastSeen && m.Removed && !cur.Removed) {
			cur.URL = m.URL
			cur.Reachable = m.Reachable
			cur.LastSeen = m.LastSeen
			cur.Removed = m.Removed
			_ = upsertMember(db, cur)
		}
	}
}

// peerURL returns the URL to use when contacting a member, honoring a
// locally verified override of its advertised URL.
func (r *Replicator) peerURL(m *member) string {
	if v, ok := r.urlOverrides.Load(m.NodeID); ok {
		return v.(string)
	}
	return m.URL
}

// notePeerErr records (and logs, on first occurrence) a failed exchange
// with a peer, so connectivity problems are visible on the dashboard
// instead of silently stalling replication.
func (r *Replicator) notePeerErr(nodeID string, err error) {
	if prev, _ := r.memberErrs.Load(nodeID); prev == nil || prev.(string) == "" {
		r.logError("sync with peer "+nodeID+" failing", err)
	}
	r.memberErrs.Store(nodeID, err.Error())
}

func (r *Replicator) clearPeerErr(nodeID string) {
	r.memberErrs.Store(nodeID, "")
}

// isHealthy reports whether a member was seen recently enough.
func (r *Replicator) isHealthy(m *member) bool {
	if m.Removed {
		return false
	}
	if m.NodeID == r.nodeID {
		return true
	}
	t, err := time.Parse(time.RFC3339, m.LastSeen)
	if err != nil {
		return false
	}
	return time.Since(t) < 3*r.cfg.SyncInterval
}
