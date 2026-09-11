package pbreplication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/security"
)

// Duplicate node id detection and healing.
//
// Every node persists its id in _repl_state on first start, so copying
// an existing node's pb_data directory to bring up a "new" node clones
// the identity too. Two nodes then run under the same id: each one's
// member table only contains itself, joins look like self-announcements
// and are silently ignored, and no data replicates - while everything
// appears healthy.
//
// Detection works on two paths:
//
//  1. Pre-serve probe: before this node starts listening, it pings the
//     seed. Any answer necessarily comes from a DIFFERENT process, so a
//     response carrying OUR node id proves the identity is duplicated.
//     Because nothing serves or captures writes yet, the identity can
//     be regenerated on the spot.
//  2. Join-time backstop (covers seeds that come up later, or two
//     clones starting simultaneously): every process carries a random,
//     non-persisted instanceID. A join between two processes that share
//     a node id but differ in instanceID is flagged; the affected node
//     regenerates its identity on its next start, before serving.
//
// Regeneration keeps all data: ops the id's original owner does not
// acknowledge are re-emitted under the fresh id so they still replicate
// out, and the old id simply becomes a regular peer.

// errDuplicateNodeID marks a join that failed because another running
// process already uses this node's persistent id (the typical aftermath
// of cloning a pb_data directory).
var errDuplicateNodeID = errors.New("duplicate node id in cluster")

const identityCheckPath = "/api/replication/identity/check"

// identityCheckRequest is used before a node exposes any listener or starts
// replication workers. Fresh nodes may not reuse any registered member name;
// restarting nodes may reclaim their persisted name only when no live process
// currently answers for it.
type identityCheckRequest struct {
	NodeID     string `json:"node_id"`
	InstanceID string `json:"instance_id"`
	URL        string `json:"url,omitempty"`
	Fresh      bool   `json:"fresh"`
}

type identityPing struct {
	NodeID     string `json:"node_id"`
	InstanceID string `json:"instance_id,omitempty"`
}

// checkStrictNodeID asks the seed to prove that the configured identity is
// available. Any failure is fatal in strict mode: starting without a positive
// answer would make uniqueness dependent on a race or a stale local database.
func (r *Replicator) checkStrictNodeID(fresh bool) error {
	if !r.cfg.StrictNodeID || r.cfg.SeedURL == "" {
		return nil
	}
	nodeID := r.nodeID
	if nodeID == "" {
		nodeID = r.cfg.NodeID
	}
	req := &identityCheckRequest{
		NodeID:     nodeID,
		InstanceID: r.instanceID,
		URL:        r.cfg.NodeURL,
		Fresh:      fresh,
	}
	if err := r.callPeer(r.cfg.SeedURL, http.MethodPost, identityCheckPath, req, nil); err != nil {
		if httpStatus(err) == http.StatusConflict {
			return fmt.Errorf("%w: configured node id %q is already in use", errDuplicateNodeID, nodeID)
		}
		if httpStatus(err) == http.StatusNotFound || httpStatus(err) == http.StatusMethodNotAllowed {
			return fmt.Errorf("pbreplication: strict node-id check is not supported by seed %s; upgrade the seed before starting this node", r.cfg.SeedURL)
		}
		return fmt.Errorf("pbreplication: cannot verify configured node id %q with seed %s: %w", nodeID, r.cfg.SeedURL, err)
	}
	return nil
}

// handleIdentityCheck answers a strict joiner's pre-start ownership check.
// It deliberately does not reserve or mutate membership; the normal join does
// that after the joining process has started listening.
func (r *Replicator) handleIdentityCheck(e *core.RequestEvent) error {
	req := &identityCheckRequest{}
	if err := e.BindBody(req); err != nil || req.NodeID == "" || req.InstanceID == "" {
		return e.BadRequestError("invalid identity check request", nil)
	}

	if req.NodeID == r.nodeID {
		// A proxy may route the request back to this same process. Only the
		// process-unique instance id makes that a valid self-check.
		if req.InstanceID == r.instanceID {
			return e.JSON(http.StatusOK, map[string]bool{"available": true})
		}
		return r.identityConflict(e, req.NodeID, "seed")
	}

	owner, err := getMember(r.app.DB(), req.NodeID)
	if err != nil {
		return e.InternalServerError("failed to inspect cluster membership", nil)
	}
	if owner == nil || owner.Removed {
		return e.JSON(http.StatusOK, map[string]bool{"available": true})
	}

	// A fresh database has no continuity proof and must not take even an
	// offline member's name. Operators can explicitly remove the old member or
	// choose another id.
	if req.Fresh {
		return r.identityConflict(e, req.NodeID, "registered member")
	}

	inUse, kind := r.registeredIdentityInUse(owner, req.InstanceID, req.URL)
	if inUse {
		return r.identityConflict(e, req.NodeID, kind)
	}
	return e.JSON(http.StatusOK, map[string]bool{"available": true})
}

// registeredIdentityInUse distinguishes a legitimate restart from a second
// live process claiming the same persisted member. A positive callback is
// authoritative. When callback verification fails, recent authenticated
// traffic remains conservative evidence that the old owner is still alive.
func (r *Replicator) registeredIdentityInUse(owner *member, joiningInstanceID, joiningURL string) (bool, string) {
	if owner.URL != "" {
		ping, err := r.identityPing(owner.URL)
		if err == nil && ping.NodeID == owner.NodeID {
			if ping.InstanceID == "" || ping.InstanceID != joiningInstanceID {
				return true, "active member"
			}
			return false, ""
		}
		// The same advertised address no longer answers, so this is the normal
		// immediate-restart case. It need not wait for the membership health TTL.
		if strings.TrimRight(joiningURL, "/") == strings.TrimRight(owner.URL, "/") {
			return false, ""
		}

		// A restart may advertise a new address while the member table still
		// contains its old one. If the new address answers with the joining
		// process's instance id, it proves this is that restart rather than a
		// second process claiming a recently active member's id.
		if joiningURL != "" {
			joiningPing, joiningErr := r.identityPing(joiningURL)
			if joiningErr == nil && joiningPing.NodeID == owner.NodeID && joiningPing.InstanceID != "" {
				if joiningPing.InstanceID == joiningInstanceID {
					return false, ""
				}
				return true, "active member at joining URL"
			}
		}
	}
	if r.isHealthy(owner) {
		kind := "recently active member"
		if owner.URL == "" {
			kind = "active pull-only member"
		}
		return true, kind
	}
	return false, ""
}

func (r *Replicator) identityPing(url string) (*identityPing, error) {
	ctx, cancel := context.WithTimeout(r.runCtx, min(r.cfg.RequestTimeout, 5*time.Second))
	defer cancel()
	ping := &identityPing{}
	err := r.callPeerCtx(ctx, url, http.MethodGet, "/api/replication/ping", nil, ping)
	return ping, err
}

func (r *Replicator) identityConflict(e *core.RequestEvent, nodeID, owner string) error {
	r.logWarn("node identity check rejected: id is already taken",
		"node", nodeID, "owner", owner)
	r.emitEvent(EventDuplicateNode, "node identity check rejected: id is already taken",
		"node", nodeID, "owner", owner)
	return e.Error(http.StatusConflict, "node id is already taken in this cluster", nil)
}

// flagDuplicateNodeID persists the detection so the NEXT process start
// regenerates this node's identity before serving. seedAck is how far
// the id's original owner acknowledges the shared sequence (ops beyond
// it are re-emitted under the new id); pass -1 when unknown.
func (r *Replicator) flagDuplicateNodeID(seedAck int64) {
	if err := setState(r.app.NonconcurrentDB(), stateDupNodePending, strconv.FormatInt(seedAck, 10)); err != nil {
		r.logError("persisting duplicate-node-id flag", err)
		return
	}
	r.logMilestone("ANOTHER CLUSTER NODE ALREADY USES THIS NODE'S ID - this node's data "+
		"directory was probably cloned from an existing node. RESTART THIS NODE to regenerate "+
		"its identity automatically (local data is kept; unsynced local writes are re-emitted "+
		"under the new id)", "node", r.nodeID)
	r.emitEvent(EventDuplicateNode, "duplicate node id detected - restart this node to regenerate its identity",
		"node", r.nodeID)
}

func (r *Replicator) clearDuplicateNodeIDFlag() error {
	value, err := getState(r.app.DB(), stateDupNodePending)
	if err != nil || value == "" {
		return err
	}
	if err := setState(r.app.NonconcurrentDB(), stateDupNodePending, ""); err != nil {
		return fmt.Errorf("clear stale duplicate-node-id flag: %w", err)
	}
	return nil
}

// resolveDuplicateNodeID runs BEFORE the node starts serving (and
// before any background loop), so it may still swap r.nodeID safely.
// It handles a duplicate flagged during a previous run and probes the
// seed for a live duplicate.
func (r *Replicator) resolveDuplicateNodeID() {
	db := r.app.NonconcurrentDB()

	// 1. flagged during a previous run (join-time backstop)
	if v, err := getState(db, stateDupNodePending); err == nil && v != "" {
		ack, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			ack = -1
		}
		if ack < 0 {
			ack = r.fetchSeedAck()
		}
		if err := r.adoptFreshNodeID(ack); err != nil {
			r.logError("regenerating node identity", err)
		}
		return
	}

	// 2. live probe. Skipped when the seed address is this node's own
	// (self-seeding can't prove anything) - the join-time backstop still
	// guards that setup.
	if r.cfg.SeedURL == "" || r.cfg.SeedURL == r.cfg.NodeURL {
		return
	}
	ctx, cancel := context.WithTimeout(r.runCtx, min(r.cfg.RequestTimeout, 5*time.Second))
	defer cancel()
	var ping struct {
		NodeID string `json:"node_id"`
	}
	if err := r.callPeerCtx(ctx, r.cfg.SeedURL, http.MethodGet, "/api/replication/ping", nil, &ping); err != nil {
		return // seed down/unreachable; nothing to prove
	}
	if ping.NodeID != r.nodeID {
		return
	}
	// We are not listening yet, so this answer came from a different
	// process claiming our id: this database is a clone.
	if err := r.adoptFreshNodeID(r.fetchSeedAck()); err != nil {
		r.logError("regenerating node identity", err)
	}
}

// fetchSeedAck asks the seed how many ops it acknowledges under this
// node's (still duplicated) id; ops beyond that are local-only and must
// be re-emitted under the fresh id. Returns -1 when the seed can't
// answer. The join request deliberately carries NO instance id so a
// duplicate-aware seed answers with its vector instead of rejecting the
// call as a duplicate.
func (r *Replicator) fetchSeedAck() int64 {
	if r.cfg.SeedURL == "" {
		return -1
	}
	req := &joinRequest{NodeID: r.nodeID}
	var resp joinResponse
	if err := r.callPeer(r.cfg.SeedURL, http.MethodPost, "/api/replication/join", req, &resp); err != nil {
		return -1
	}
	if resp.Vector == nil {
		return -1
	}
	return resp.Vector[r.nodeID]
}

// adoptFreshNodeID rewrites the local database so this node stops
// claiming the duplicated id and continues as a brand-new member:
//
//   - ops the id's original owner does not acknowledge (src_seq >
//     seedAck) are re-emitted under the fresh id with fresh sequence
//     numbers (original HLCs kept, so LWW outcomes don't change) and
//     the duplicated rows are dropped
//   - the old id becomes a regular peer: its vector entry is set to
//     what our oplog still holds of it, so anti-entropy pulls only what
//     the original owner wrote after the clone was taken
//   - the LWW version rows of the re-emitted writes follow the new id
//     so equal-HLC tiebreaks stay consistent cluster-wide
//
// A negative seedAck means the boundary is unknown; everything held
// under the old id is then re-emitted (idempotent on peers - the LWW
// gate skips ops they already applied).
//
// MUST run before the node serves or captures writes, because it swaps
// r.nodeID.
func (r *Replicator) adoptFreshNodeID(seedAck int64) error {
	oldID := r.nodeID
	// always random - a configured Config.NodeID got us into the
	// duplicate in the first place (both twins carry the same config),
	// and any non-random choice could collide with existing oplog history
	newID := security.RandomString(15)

	reemitted := 0
	err := r.app.RunInTransaction(func(txApp core.App) error {
		db := txApp.NonconcurrentDB()

		var maxOwn int64
		if err := db.NewQuery(`SELECT COALESCE(MAX(src_seq), 0) FROM _repl_oplog WHERE src_node = {:s}`).
			Bind(dbx.Params{"s": oldID}).Row(&maxOwn); err != nil {
			return err
		}
		ack := seedAck
		if ack < 0 {
			ack = 0 // unknown boundary: convergence beats economy
		}
		if ack > maxOwn {
			ack = maxOwn
		}

		var rows []oplogRow
		if err := db.NewQuery(`SELECT rowid, * FROM _repl_oplog
			WHERE src_node = {:s} AND src_seq > {:a} ORDER BY src_seq`).
			Bind(dbx.Params{"s": oldID, "a": ack}).All(&rows); err != nil {
			return err
		}
		for i := range rows {
			o := rows[i].toOp()
			o.SrcNode = newID
			o.SrcSeq = int64(i + 1)
			if err := insertOp(db, o); err != nil {
				return err
			}
			verCol := o.ColID
			if o.Type == opColUpsert || o.Type == opColDelete {
				verCol = collectionsColID
			}
			if _, err := db.NewQuery(`UPDATE _repl_versions SET src_node = {:new}
				WHERE col_id = {:c} AND record_id = {:r} AND src_node = {:old} AND hlc = {:h}`).
				Bind(dbx.Params{"new": newID, "c": verCol, "r": o.RecordID, "old": oldID, "h": o.HLC}).
				Execute(); err != nil {
				return err
			}
		}
		reemitted = len(rows)
		if _, err := db.NewQuery(`DELETE FROM _repl_oplog WHERE src_node = {:s} AND src_seq > {:a}`).
			Bind(dbx.Params{"s": oldID, "a": ack}).Execute(); err != nil {
			return err
		}

		if err := setState(db, stateNodeID, newID); err != nil {
			return err
		}
		if err := setState(db, stateLocalSeq, strconv.Itoa(reemitted)); err != nil {
			return err
		}
		// the old id is a regular peer from now on
		if err := setState(db, stateVectorPrefix+oldID, strconv.FormatInt(ack, 10)); err != nil {
			return err
		}
		if _, err := db.NewQuery(`DELETE FROM _repl_state WHERE key = {:k}`).
			Bind(dbx.Params{"k": stateVectorPrefix + newID}).Execute(); err != nil {
			return err
		}
		if err := setState(db, stateDupNodePending, ""); err != nil {
			return err
		}

		// The old id's member row may still carry OUR advertised URL (a
		// previous run of this clone overwrote it in startBackground).
		// Clear it; the real owner's URL comes back with the next
		// join/gossip merge.
		if old, err := getMember(db, oldID); err == nil && old != nil && old.URL == r.cfg.NodeURL {
			old.URL = ""
			old.Reachable = false
			if err := upsertMember(db, old); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	r.nodeID = newID
	// validate relation integrity once the post-heal deltas settle
	r.scheduleIntegrityCheck()
	r.logMilestone("node identity regenerated after duplicate node id detection "+
		"(this node's data directory was likely cloned from an existing node)",
		"old_id", oldID, "new_id", newID, "reemitted_ops", reemitted)
	r.emitEvent(EventDuplicateNode, "node identity regenerated (duplicate id, likely a cloned data directory)",
		"old_id", oldID, "new_id", newID, "reemitted_ops", reemitted)
	return nil
}
