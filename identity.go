package pbreplication

import (
	"context"
	"errors"
	"net/http"
	"strconv"
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
