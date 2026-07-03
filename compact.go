package pbreplication

import (
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// compactLoop runs the garbage collection every CompactionInterval.
func (r *Replicator) compactLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.cfg.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if !r.ready.Load() {
				continue
			}
			if err := r.compact(); err != nil {
				r.logError("compaction failed", err)
			}
		}
	}
}

// compact garbage-collects every replication table so nothing grows
// forever:
//
//   - superseded oplog entries (ops for a record that has a strictly
//     newer op) are dropped after a grace period. This is always safe:
//     a superseded op's effect is fully covered by the retained newest
//     op, and the LWW gate makes replaying it a no-op anyway.
//   - delete tombstone ops are dropped after TombstoneRetention. This
//     is the one deletion that can lose information (a peer offline for
//     longer would never learn about the delete), so the highest
//     dropped tombstone sequence per source is recorded in
//     _repl_compaction; pulls reaching before that horizon receive
//     snapshot_required and full-resync instead.
//   - _repl_versions tombstone rows are purged after TombstoneRetention.
//   - members not seen for TombstoneRetention are flagged removed and
//     purged (with their vector/compaction state) after 2x retention.
func (r *Replicator) compact() error {
	return r.app.RunInTransaction(func(txApp core.App) error {
		db := txApp.NonconcurrentDB()

		now := time.Now().UTC()
		graceCutoff := now.Add(-r.cfg.CompactionInterval).Format(time.RFC3339)
		tombstoneCutoff := now.Add(-r.cfg.TombstoneRetention).Format(time.RFC3339)
		purgeCutoff := now.Add(-2 * r.cfg.TombstoneRetention).Format(time.RFC3339)

		// 1. drop superseded record ops (keep the newest per record) ...
		_, err := db.NewQuery(`DELETE FROM _repl_oplog
			WHERE op_type IN ('upsert','delete') AND created < {:grace} AND EXISTS (
				SELECT 1 FROM _repl_oplog n
				WHERE n.op_type IN ('upsert','delete')
				  AND n.col_id = _repl_oplog.col_id
				  AND n.record_id = _repl_oplog.record_id
				  AND (n.hlc > _repl_oplog.hlc OR (n.hlc = _repl_oplog.hlc AND n.src_node > _repl_oplog.src_node))
			)`).Bind(dbx.Params{"grace": graceCutoff}).Execute()
		if err != nil {
			return err
		}

		// ... and superseded collection ops (keep the newest per collection)
		_, err = db.NewQuery(`DELETE FROM _repl_oplog
			WHERE op_type IN ('col_upsert','col_delete') AND created < {:grace} AND EXISTS (
				SELECT 1 FROM _repl_oplog n
				WHERE n.op_type IN ('col_upsert','col_delete')
				  AND n.record_id = _repl_oplog.record_id
				  AND (n.hlc > _repl_oplog.hlc OR (n.hlc = _repl_oplog.hlc AND n.src_node > _repl_oplog.src_node))
			)`).Bind(dbx.Params{"grace": graceCutoff}).Execute()
		if err != nil {
			return err
		}

		// 2. record the tombstone horizon per source, then drop expired
		//    tombstone ops
		type srcMax struct {
			SrcNode string `db:"src_node"`
			MaxSeq  int64  `db:"max_seq"`
		}
		var horizons []srcMax
		err = db.NewQuery(`SELECT src_node, MAX(src_seq) AS max_seq FROM _repl_oplog
			WHERE op_type IN ('delete','col_delete') AND created < {:cut}
			GROUP BY src_node`).Bind(dbx.Params{"cut": tombstoneCutoff}).All(&horizons)
		if err != nil {
			return err
		}
		for _, h := range horizons {
			_, err = db.NewQuery(`INSERT INTO _repl_compaction (src_node, min_seq) VALUES ({:s}, {:m})
				ON CONFLICT(src_node) DO UPDATE SET min_seq = MAX(min_seq, {:m})`).
				Bind(dbx.Params{"s": h.SrcNode, "m": h.MaxSeq}).Execute()
			if err != nil {
				return err
			}
		}
		_, err = db.NewQuery(`DELETE FROM _repl_oplog
			WHERE op_type IN ('delete','col_delete') AND created < {:cut}`).
			Bind(dbx.Params{"cut": tombstoneCutoff}).Execute()
		if err != nil {
			return err
		}

		// 3. purge expired version tombstones (live rows stay - they
		//    are the LWW state, bounded by the record count)
		_, err = db.NewQuery(`DELETE FROM _repl_versions WHERE deleted = 1 AND updated < {:cut}`).
			Bind(dbx.Params{"cut": tombstoneCutoff}).Execute()
		if err != nil {
			return err
		}

		// 4. flag long-gone members as removed...
		_, err = db.NewQuery(`UPDATE _repl_members SET removed = 1
			WHERE node_id != {:self} AND removed = 0 AND last_seen < {:cut}`).
			Bind(dbx.Params{"self": r.nodeID, "cut": tombstoneCutoff}).Execute()
		if err != nil {
			return err
		}

		// ...and purge them (plus their bookkeeping) after 2x retention
		var purged []string
		err = db.NewQuery(`SELECT node_id FROM _repl_members
			WHERE node_id != {:self} AND removed = 1 AND last_seen < {:cut}`).
			Bind(dbx.Params{"self": r.nodeID, "cut": purgeCutoff}).Column(&purged)
		if err != nil {
			return err
		}
		for _, nodeID := range purged {
			if _, err = db.NewQuery(`DELETE FROM _repl_members WHERE node_id = {:n}`).
				Bind(dbx.Params{"n": nodeID}).Execute(); err != nil {
				return err
			}
			if _, err = db.NewQuery(`DELETE FROM _repl_state WHERE key = {:k}`).
				Bind(dbx.Params{"k": stateVectorPrefix + nodeID}).Execute(); err != nil {
				return err
			}
			if _, err = db.NewQuery(`DELETE FROM _repl_compaction WHERE src_node = {:n}`).
				Bind(dbx.Params{"n": nodeID}).Execute(); err != nil {
				return err
			}
		}

		// 6. prune stale/excess client IP rows (dashboard map data)
		return gcClients(db, r.cfg.TombstoneRetention)
	})
}
