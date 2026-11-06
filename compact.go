package pbreplication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

const (
	// stateGCPass holds the durable cursor of the current GC pass.
	stateGCPass = "gc_pass"
	// stateGCLegacyProgress is an unreleased earlier cursor format. It is
	// cleared when a new pass starts.
	stateGCLegacyProgress = "gc_progress"
	// stateGCHLC is the scalar GC fence: the highest HLC of any collected
	// tombstone. Operations at or behind it cannot create absent state.
	stateGCHLC = "gc_hlc"
)

// ErrCompactionRunning is returned by Compact while another compaction
// invocation holds the collector.
var ErrCompactionRunning = errors.New("pbreplication: compaction already running")

// ErrCompactionNotReady is returned by Compact before replication storage
// has been initialized.
var ErrCompactionNotReady = errors.New("pbreplication: replication storage is not ready")

const (
	gcPhaseOplog = iota
	gcPhaseVersions
	gcPhaseMembers
	gcPhaseClients
	gcPhasePaths
	gcPhaseCount
)

var (
	gcPhaseNames  = [gcPhaseCount]string{"oplog", "versions", "members", "clients", "paths"}
	gcPhaseTables = [gcPhaseCount]string{"_repl_oplog", "_repl_versions", "_repl_members", "_repl_client_ips", "_repl_client_paths"}
)

// gcProgressLogEvery throttles the informational progress line of a pass
// that spans many invocations.
const gcProgressLogEvery = 5 * time.Minute

// CompactionStats counts committed work in one bounded Compact invocation.
type CompactionStats struct {
	Batches  int
	Scanned  int
	Deleted  int
	Repaired int
	Flagged  int
	// Complete reports that this invocation finished a whole pass over
	// every table. It does not mean concurrent writes stopped.
	Complete bool
}

type gcCursor struct {
	After   int64 `json:"after"`
	Head    int64 `json:"head"`
	Started bool  `json:"started"`
	Done    bool  `json:"done"`
}

// gcPass is the durable state of one GC pass. Phases are visited
// round-robin, one bounded batch at a time (see nextPhase). Each phase scans a finite rowid
// range captured when the phase first runs.
type gcPass struct {
	Now      string                 `json:"now"`
	Next     int                    `json:"next"`
	Cursors  [gcPhaseCount]gcCursor `json:"cursors"`
	Batches  int64                  `json:"batches"`
	Scanned  int64                  `json:"scanned"`
	Deleted  int64                  `json:"deleted"`
	Repaired int64                  `json:"repaired"`
	Flagged  int64                  `json:"flagged"`
}

func (p *gcPass) complete() bool {
	for _, c := range p.Cursors {
		if !c.Done {
			return false
		}
	}
	return true
}

// nextPhase returns the first runnable unfinished phase at or after from.
// The versions phase waits for the oplog phase: a version tombstone can only
// be collected once its expired delete operation is gone, so this ordering
// lets one pass converge. The small tables interleave with the oplog phase
// and are never starved behind a large oplog backlog.
func (p *gcPass) nextPhase(from int) int {
	for i := 0; i < gcPhaseCount; i++ {
		phase := (from + i) % gcPhaseCount
		if p.Cursors[phase].Done || (phase == gcPhaseVersions && !p.Cursors[gcPhaseOplog].Done) {
			continue
		}
		return phase
	}
	return -1
}

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
			_, err := r.Compact(r.runCtx)
			if err != nil && !errors.Is(err, ErrCompactionRunning) && !errors.Is(err, context.Canceled) {
				r.logError("compaction failed", err)
			}
			r.cleanupDBSnapshots()
		}
	}
}

func (r *Replicator) compact() error {
	_, err := r.Compact(context.Background())
	return err
}

// Compact performs a bounded, resumable garbage-collection step. Each batch
// commits independently, releasing the SQLite writer. Cancellation rolls back
// only the current batch; durable progress resumes on the next invocation.
// It must not be called inside a host write transaction.
func (r *Replicator) Compact(ctx context.Context) (stats CompactionStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !r.ready.Load() || r.nodeID == "" {
		return stats, ErrCompactionNotReady
	}
	if !r.compactMu.TryLock() {
		return stats, ErrCompactionRunning
	}
	defer r.compactMu.Unlock()
	start := time.Now()
	defer func() {
		r.log(slog.LevelDebug, "compaction run", "batches", stats.Batches, "scanned", stats.Scanned,
			"deleted", stats.Deleted, "repaired", stats.Repaired, "flagged", stats.Flagged,
			"complete", stats.Complete, "duration", time.Since(start).String())
	}()
	for stats.Batches < r.cfg.CompactionMaxBatches && time.Since(start) < r.cfg.CompactionTimeBudget {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		select {
		case <-r.stopCh:
			return stats, context.Canceled
		default:
		}
		var batch CompactionStats
		var phase int
		var removed []string
		var pass gcPass
		err = r.app.RunInTransaction(func(app core.App) error {
			var err error
			phase, err = r.compactBatch(ctx, app, &pass, &batch, &removed)
			return err
		})
		if err != nil {
			return stats, err
		}
		stats.Batches++
		stats.Scanned += batch.Scanned
		stats.Deleted += batch.Deleted
		stats.Repaired += batch.Repaired
		stats.Flagged += batch.Flagged
		stats.Complete = batch.Complete
		if batch.Repaired > 0 {
			// only after commit, so the pusher can see the repair tombstones
			wake(r.pushWake)
		}
		for _, nodeID := range removed {
			r.logMilestone("member removed from cluster (not seen within tombstone retention)", "peer", nodeID)
			r.emitEvent(EventNodeRemoved, "member removed from cluster", "peer", nodeID)
		}
		r.log(slog.LevelDebug, "compaction batch", "phase", gcPhaseNames[phase], "scanned", batch.Scanned,
			"deleted", batch.Deleted, "repaired", batch.Repaired, "flagged", batch.Flagged)
		if stats.Complete {
			duration := ""
			if began, err := time.Parse(time.RFC3339, pass.Now); err == nil {
				duration = time.Since(began).Round(time.Second).String()
			}
			r.logMilestone("compaction pass complete", "batches", pass.Batches, "scanned", pass.Scanned,
				"deleted", pass.Deleted, "repaired", pass.Repaired, "flagged", pass.Flagged, "duration", duration)
			return stats, nil
		}
		if r.throttleOK("gc:progress", gcProgressLogEvery) {
			r.logInfo("compaction pass in progress", "phase", gcPhaseNames[phase],
				"batches", pass.Batches, "scanned", pass.Scanned, "deleted", pass.Deleted,
				"repaired", pass.Repaired, "flagged", pass.Flagged, "started", pass.Now)
		}
		runtime.Gosched()
	}
	return stats, nil
}

// Planner statistics guard. SQLite has no INDEXED BY for rowid ranges: when
// sqlite_stat1 was collected while a table was tiny (PocketBase runs
// PRAGMA optimize daily and after schema changes), the planner scans the
// whole table for every rowid-keyset read, which makes a GC pass quadratic
// and every push batch a full oplog scan. Each GC phase and the pusher
// (throttled) therefore re-analyze a table, with a bounded sample, when its
// statistics underestimate its rowid span by staleStatsFactor or more. The
// refreshed sqlite_stat1 changes the schema cookie, so every pooled
// connection re-plans.
const (
	staleStatsFactor   = 10
	staleStatsMinRows  = 1000
	gcAnalysisRowLimit = 400
	statsCheckInterval = time.Minute
)

// statsStale reports whether table's planner statistics underestimate its
// rowid span by staleStatsFactor or more. Missing statistics are not stale:
// the planner's defaults favour keyset seeks.
func statsStale(db dbx.Builder, table string) (bool, error) {
	var hasStatTable bool
	if err := db.NewQuery("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'sqlite_stat1')").
		Row(&hasStatTable); err != nil || !hasStatTable {
		return false, err
	}
	var stat string
	err := db.NewQuery("SELECT stat FROM sqlite_stat1 WHERE tbl = {:t} LIMIT 1").
		Bind(dbx.Params{"t": table}).Row(&stat)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var estimated int64
	fmt.Sscanf(stat, "%d", &estimated)
	var span int64
	if err := db.NewQuery("SELECT COALESCE(MAX(rowid) - MIN(rowid) + 1, 0) FROM " + table).Row(&span); err != nil {
		return false, err
	}
	return span >= staleStatsMinRows && span >= staleStatsFactor*estimated, nil
}

// analyzeBounded runs ANALYZE on table with a bounded sample, restoring the
// connection's previous analysis_limit. db must be a single connection (the
// non-concurrent pool or a transaction).
func analyzeBounded(db dbx.Builder, table string) error {
	var limit int64
	if err := db.NewQuery("PRAGMA analysis_limit").Row(&limit); err != nil {
		return err
	}
	if _, err := db.NewQuery(fmt.Sprintf("PRAGMA analysis_limit = %d", gcAnalysisRowLimit)).Execute(); err != nil {
		return err
	}
	_, analyzeErr := db.NewQuery("ANALYZE " + table).Execute()
	if _, err := db.NewQuery(fmt.Sprintf("PRAGMA analysis_limit = %d", limit)).Execute(); err != nil && analyzeErr == nil {
		analyzeErr = err
	}
	return analyzeErr
}

// refreshStaleStats re-analyzes table on db when its statistics are stale.
func refreshStaleStats(db dbx.Builder, table string) error {
	stale, err := statsStale(db, table)
	if err != nil || !stale {
		return err
	}
	return analyzeBounded(db, table)
}

// refreshOplogStats keeps the push path's rowid-range reads on the rowid
// seek. It checks at most once per statsCheckInterval, on the read pool, and
// only takes the write connection when the statistics are actually stale.
func (r *Replicator) refreshOplogStats() {
	now := time.Now().UnixNano()
	last := r.statsCheckAt.Load()
	if now-last < int64(statsCheckInterval) || !r.statsCheckAt.CompareAndSwap(last, now) {
		return
	}
	stale, err := statsStale(r.app.DB(), "_repl_oplog")
	if err == nil && stale {
		err = analyzeBounded(r.app.NonconcurrentDB(), "_repl_oplog")
	}
	if err != nil {
		r.logError("refresh oplog planner statistics", err)
	}
}

// compactBatch runs one bounded batch of the current pass inside the given
// write transaction and persists the advanced cursor in the same transaction.
func (r *Replicator) compactBatch(ctx context.Context, app core.App, pass *gcPass, batch *CompactionStats, removed *[]string) (int, error) {
	db := app.NonconcurrentDB()
	raw, err := getState(db, stateGCPass)
	if err != nil {
		return 0, err
	}
	p := gcPass{Now: time.Now().UTC().Format(time.RFC3339Nano)}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return 0, fmt.Errorf("pbreplication: invalid GC cursor: %w", err)
		}
	} else if legacy, err := getState(db, stateGCLegacyProgress); err != nil {
		return 0, err
	} else if legacy != "" {
		if err := setState(db, stateGCLegacyProgress, ""); err != nil {
			return 0, err
		}
	}
	if p.Next < 0 || p.Next >= gcPhaseCount {
		return 0, errors.New("pbreplication: invalid GC phase")
	}
	phase := p.nextPhase(p.Next)
	if phase < 0 {
		return 0, errors.New("pbreplication: GC cursor has no unfinished phase")
	}
	at, err := time.Parse(time.RFC3339, p.Now)
	if err != nil {
		return phase, err
	}
	c := &p.Cursors[phase]
	table := gcPhaseTables[phase]
	if !c.Started {
		if err := refreshStaleStats(db, table); err != nil {
			return phase, err
		}
		if err := db.NewQuery("SELECT COALESCE(MAX(rowid), 0) FROM " + table).Row(&c.Head); err != nil {
			return phase, err
		}
		c.Started = true
	}
	var ids []int64
	if err := db.NewQuery("SELECT rowid FROM " + table +
		" WHERE rowid > {:after} AND rowid <= {:head} ORDER BY rowid LIMIT {:limit}").
		Bind(dbx.Params{"after": c.After, "head": c.Head, "limit": r.cfg.CompactionBatchSize}).Column(&ids); err != nil {
		return phase, err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return phase, err
		}
		if err := r.gcRow(app, phase, id, at, batch, removed); err != nil {
			return phase, err
		}
		c.After = id
		batch.Scanned++
	}
	if err := ctx.Err(); err != nil {
		return phase, err
	}
	if len(ids) < r.cfg.CompactionBatchSize || c.After >= c.Head {
		c.Done = true
	}
	p.Batches++
	p.Scanned += int64(batch.Scanned)
	p.Deleted += int64(batch.Deleted)
	p.Repaired += int64(batch.Repaired)
	p.Flagged += int64(batch.Flagged)
	*pass = p
	if p.complete() {
		batch.Complete = true
		return phase, setState(db, stateGCPass, "")
	}
	p.Next = p.nextPhase(phase + 1)
	*pass = p
	data, err := json.Marshal(p)
	if err != nil {
		return phase, err
	}
	return phase, setState(db, stateGCPass, string(data))
}

func (r *Replicator) gcRow(app core.App, phase int, id int64, now time.Time, stats *CompactionStats, removed *[]string) error {
	db := app.NonconcurrentDB()
	cut := now.Add(-r.cfg.TombstoneRetention).Format(time.RFC3339)
	switch phase {
	case gcPhaseOplog:
		var row oplogRow
		if err := db.NewQuery("SELECT rowid, * FROM _repl_oplog WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).One(&row); err != nil {
			return err
		}
		collection := row.OpType == opColUpsert || row.OpType == opColDelete
		newer, err := newerOpExists(db, collection, row.ColID, row.RecordID, row.HLC, row.SrcNode)
		if err != nil {
			return err
		}
		expiredDelete := (row.OpType == opDelete || row.OpType == opColDelete) && row.Created < cut
		superseded := newer && row.Created < now.Add(-r.cfg.CompactionInterval).Format(time.RFC3339)
		if expiredDelete || superseded {
			if expiredDelete {
				// Peers that have not yet seen this delete must resync:
				// the snapshot horizon is exactly the dropped delete.
				r.clock.Observe(row.HLC)
				if err := gcHorizon(db, "_repl_compaction", row.SrcNode, row.SrcSeq); err != nil {
					return err
				}
				if err := gcHLCHorizon(db, row.HLC); err != nil {
					return err
				}
			}
			if _, err := db.NewQuery("DELETE FROM _repl_oplog WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).Execute(); err != nil {
				return err
			}
			stats.Deleted++
		} else if !newer && row.Created < cut && (row.OpType == opUpsert || row.OpType == opColUpsert) {
			colID := row.ColID
			if collection {
				colID = collectionsColID
			}
			repaired, err := r.repairOrphan(app, colID, row.RecordID, row.HLC, row.SrcNode,
				row.SrcNode == r.nodeID, row.Created)
			if err != nil {
				return err
			}
			if repaired {
				stats.Repaired++
			}
		}
	case gcPhaseVersions:
		var v versionRow
		if err := db.NewQuery("SELECT * FROM _repl_versions WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).One(&v); err != nil {
			return err
		}
		collection := v.ColID == collectionsColID
		if !v.Deleted {
			repaired, err := r.repairOrphan(app, v.ColID, v.RecordID, v.HLC, v.SrcNode, false, v.Updated)
			if err != nil {
				return err
			}
			if repaired {
				stats.Repaired++
			}
		} else if v.Updated < cut {
			remains, err := opsRemain(db, collection, v.ColID, v.RecordID)
			if err != nil {
				return err
			}
			if !remains {
				// The tombstone's own delete op is gone (its horizon was
				// recorded when phase 0 dropped it). Without LWW state, the
				// scalar fence keeps older operations from recreating it.
				r.clock.Observe(v.HLC)
				if err := gcHLCHorizon(db, v.HLC); err != nil {
					return err
				}
				if _, err := db.NewQuery("DELETE FROM _repl_versions WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).Execute(); err != nil {
					return err
				}
				stats.Deleted++
			}
		}
	case gcPhaseMembers:
		var m member
		if err := db.NewQuery("SELECT * FROM _repl_members WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).One(&m); err != nil {
			return err
		}
		if m.NodeID == r.nodeID || m.LastSeen >= cut {
			return nil
		}
		if !m.Removed {
			_, err := db.NewQuery("UPDATE _repl_members SET removed = 1 WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).Execute()
			stats.Flagged++
			*removed = append(*removed, m.NodeID)
			return err
		}
		if m.LastSeen < now.Add(-2*r.cfg.TombstoneRetention).Format(time.RFC3339) {
			if _, err := db.NewQuery("DELETE FROM _repl_members WHERE rowid = {:id}").Bind(dbx.Params{"id": id}).Execute(); err != nil {
				return err
			}
			// Keep source vectors and GC horizons: membership removal is
			// not permission to replay or resurrect its expired operations.
			stats.Deleted++
		}
	case gcPhaseClients:
		result, err := db.NewQuery(clientsGCSQL).
			Bind(dbx.Params{"id": id, "cut": cut, "cap": maxTrackedClients - 1}).Execute()
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		stats.Deleted += int(n)
		return err
	case gcPhasePaths:
		result, err := db.NewQuery(pathsGCSQL).
			Bind(dbx.Params{"id": id, "cap": maxPathsPerIP - 1}).Execute()
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		stats.Deleted += int(n)
		return err
	}
	return nil
}

// oplogKey returns the indexed (col_id, record_id) pair and op types of the
// LWW key. Collection operations always carry col_id == record_id == the
// collection id, so both kinds are served by _repl_oplog_rec_idx.
func oplogKey(collection bool, colID, recordID string) (string, string) {
	if collection {
		return recordID, "('col_upsert','col_delete')"
	}
	return colID, "('upsert','delete')"
}

// The per-candidate oplog checks pin _repl_oplog_rec_idx with INDEXED BY:
// stale sqlite_stat1 statistics (e.g. collected while the oplog was tiny)
// otherwise make the planner fall back to a full-table scan per candidate.
// With INDEXED BY, an unusable index is a hard error instead of a silent scan.

func newerOpSQL(types string) string {
	return `SELECT EXISTS(SELECT 1 FROM _repl_oplog INDEXED BY _repl_oplog_rec_idx
		WHERE col_id = {:col} AND record_id = {:rec} AND op_type IN ` + types + `
		AND hlc >= {:hlc} AND (hlc > {:hlc} OR src_node > {:src}))`
}

func opsRemainSQL(types string) string {
	return `SELECT EXISTS(SELECT 1 FROM _repl_oplog INDEXED BY _repl_oplog_rec_idx
		WHERE col_id = {:col} AND record_id = {:rec} AND op_type IN ` + types + `)`
}

const retainedDeleteSQL = `SELECT EXISTS(SELECT 1 FROM _repl_oplog INDEXED BY _repl_oplog_rec_idx
	WHERE col_id = {:col} AND record_id = {:rec} AND op_type = {:type} AND hlc >= {:hlc})`

// clientsGCSQL deletes one candidate client IP row when it is stale or
// ranks beyond the tracked-client cap.
const clientsGCSQL = `DELETE FROM _repl_client_ips WHERE rowid = {:id} AND (
	last_seen < {:cut} OR EXISTS (
		SELECT 1 FROM _repl_client_ips p INDEXED BY _repl_client_ips_gc_idx
		WHERE (p.last_seen, p.ip) > (_repl_client_ips.last_seen, _repl_client_ips.ip)
		ORDER BY last_seen DESC, ip DESC LIMIT 1 OFFSET {:cap}))`

// pathsGCSQL deletes one candidate client path row when its IP is no longer
// tracked or it ranks beyond the per-IP path cap.
const pathsGCSQL = `DELETE FROM _repl_client_paths WHERE rowid = {:id} AND (
	NOT EXISTS (SELECT 1 FROM _repl_client_ips i WHERE i.ip = _repl_client_paths.ip)
	OR EXISTS (SELECT 1 FROM _repl_client_paths p INDEXED BY _repl_client_paths_gc_idx
		WHERE p.ip = _repl_client_paths.ip
		AND (p.count, p.method, p.path) > (_repl_client_paths.count, _repl_client_paths.method, _repl_client_paths.path)
		ORDER BY count DESC, method DESC, path DESC LIMIT 1 OFFSET {:cap}))`

// newerOpExists reports whether the oplog holds an operation for the same
// LWW key ordered after (hlc, src).
func newerOpExists(db dbx.Builder, collection bool, colID, recordID, hlc, src string) (bool, error) {
	col, types := oplogKey(collection, colID, recordID)
	var newer bool
	err := db.NewQuery(newerOpSQL(types)).
		Bind(dbx.Params{"col": col, "rec": recordID, "hlc": hlc, "src": src}).Row(&newer)
	return newer, err
}

// opsRemain reports whether any operation for the LWW key is still retained.
func opsRemain(db dbx.Builder, collection bool, colID, recordID string) (bool, error) {
	col, types := oplogKey(collection, colID, recordID)
	var remains bool
	err := db.NewQuery(opsRemainSQL(types)).
		Bind(dbx.Params{"col": col, "rec": recordID}).Row(&remains)
	return remains, err
}

func gcHorizon(db dbx.Builder, table, source string, seq int64) error {
	_, err := db.NewQuery("INSERT INTO " + table + ` (src_node, min_seq) VALUES ({:src}, {:seq})
		ON CONFLICT(src_node) DO UPDATE SET min_seq = MAX(min_seq, excluded.min_seq)`).
		Bind(dbx.Params{"src": source, "seq": seq}).Execute()
	return err
}

func gcHLCHorizon(db dbx.Builder, hlc string) error {
	floor, err := getState(db, stateGCHLC)
	if err != nil || hlc <= floor {
		return err
	}
	return setState(db, stateGCHLC, hlc)
}

// repairOrphan converts a record (or collection) that was removed locally
// without capture into an ordinary fresh local tombstone, so the deletion
// replicates and receives the full TombstoneRetention. It runs under the
// same writer transaction as the existence checks.
//
// A repair is a winning cluster-wide delete, so it requires positive local
// evidence that the state existed here: a local LWW version, or a locally
// originated operation (localOrigin). A remote operation that never applied
// locally (parked, failed, conflicting) is not evidence of a deletion.
// Record repairs also require the collection to resolve by its exact id and
// to be the incarnation the evidence belongs to (seenAt is the version's
// update time, or the operation's creation time when no version exists).
func (r *Replicator) repairOrphan(app core.App, colID, id, oldHLC, oldSource string, localOrigin bool, seenAt string) (bool, error) {
	db := app.NonconcurrentDB()
	v, err := getVersion(db, colID, id)
	if err != nil {
		return false, err
	}
	if v == nil && !localOrigin {
		return false, nil
	}
	if v != nil {
		seenAt = v.Updated
	}
	collection := colID == collectionsColID
	var col *core.Collection
	if collection {
		if v == nil {
			// a failed schema apply never writes a version
			return false, nil
		}
		if _, err := app.FindCollectionByNameOrId(id); err == nil {
			return false, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	} else {
		col, err = app.FindCachedCollectionByNameOrId(colID)
		if err != nil || col == nil || col.Id != colID {
			// deleted, unknown, or only name-resolved collection: the
			// record's absence proves nothing about a deletion
			return false, nil
		}
		if !r.isReplicated(col) {
			return false, nil
		}
		if seen, err := time.Parse(time.RFC3339, seenAt); err != nil ||
			seen.Before(col.Created.Time().Truncate(time.Second)) {
			// evidence from an earlier incarnation of a recreated collection
			return false, nil
		}
		if _, err := app.FindRecordById(col, id); err == nil {
			return false, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	}
	if v != nil && v.Deleted {
		deleteType := opDelete
		opColID := colID
		if collection {
			deleteType = opColDelete
			opColID = id
		}
		var retainedDelete bool
		if err := db.NewQuery(retainedDeleteSQL).
			Bind(dbx.Params{"col": opColID, "rec": id, "type": deleteType, "hlc": v.HLC}).Row(&retainedDelete); err != nil {
			return false, err
		}
		if retainedDelete {
			return false, nil
		}
	}
	// Do not overtake an operation waiting in the async applier.
	checkHLC, checkSource := oldHLC, oldSource
	if v != nil {
		checkHLC, checkSource = v.HLC, v.SrcNode
	}
	newer, err := newerOpExists(db, collection, colID, id, checkHLC, checkSource)
	if err != nil || newer {
		return false, err
	}
	seq, err := incrLocalSeq(db)
	if err != nil {
		return false, err
	}
	r.clock.Observe(oldHLC)
	if v != nil {
		r.clock.Observe(v.HLC)
	}
	o := &op{SrcNode: r.nodeID, SrcSeq: seq, HLC: r.clock.Now(), Type: opDelete, ColID: colID, RecordID: id}
	if col != nil {
		o.ColName = col.Name
	}
	if collection {
		o.Type, o.ColID = opColDelete, id
	}
	if err := insertOp(db, o); err != nil {
		return false, err
	}
	if err := upsertVersion(db, colID, id, o.HLC, o.SrcNode, true); err != nil {
		return false, err
	}
	return true, nil
}

// gcRejects rejects replay of collected state only when no LWW state remains.
// Logical snapshot operations (SrcSeq == 0) must still restore old live records.
func gcRejects(db dbx.Builder, o *op, cur *versionRow) (bool, error) {
	if o.SrcSeq <= 0 || cur != nil {
		return false, nil
	}
	// Already admitted operations may still be queued in the async applier.
	// A later fence must not mistake them for replay of collected state.
	var admitted bool
	if err := db.NewQuery("SELECT EXISTS(SELECT 1 FROM _repl_oplog WHERE src_node = {:s} AND src_seq = {:q})").
		Bind(dbx.Params{"s": o.SrcNode, "q": o.SrcSeq}).Row(&admitted); err != nil {
		return false, err
	}
	if admitted {
		return false, nil
	}
	return behindGCFence(db, o, cur)
}
