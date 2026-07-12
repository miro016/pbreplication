package pbreplication

import (
	"database/sql"
	"time"
)

// SyncPhase names what the replication engine is currently busy with.
type SyncPhase string

const (
	// SyncIdle: steady-state incremental replication only.
	SyncIdle SyncPhase = "idle"
	// SyncCopying: downloading a full database snapshot file from a peer.
	SyncCopying SyncPhase = "copying"
	// SyncSnapshotting: running a logical (row-by-row) snapshot sync.
	SyncSnapshotting SyncPhase = "snapshotting"
	// SyncResyncing: running a logical snapshot WITH reconcile (deletes
	// of records missing on the peer), after falling behind compaction.
	SyncResyncing SyncPhase = "resyncing"
	// SyncBlobBackfill: fetching file blobs referenced by copied records.
	SyncBlobBackfill SyncPhase = "blob_backfill"
	// SyncIntegrityCheck: validating relation integrity after a sync.
	SyncIntegrityCheck SyncPhase = "integrity_check"
)

// SyncStatus is a live snapshot of a bulk synchronization in progress
// (initial sync, resync, full DB copy). Zero-valued/Phase==SyncIdle when
// nothing bulk is running.
type SyncStatus struct {
	Phase      SyncPhase     `json:"phase"`
	Peer       string        `json:"peer,omitempty"`       // peer the data comes from
	Collection string        `json:"collection,omitempty"` // collection currently transferring
	DoneRows   int64         `json:"done_rows"`
	TotalRows  int64         `json:"total_rows"` // 0 = unknown
	Percent    int           `json:"percent"`
	ETA        time.Duration `json:"-"`
	ETAString  string        `json:"eta,omitempty"`
	BytesDone  int64         `json:"bytes_done,omitempty"`  // full-copy phase only
	BytesTotal int64         `json:"bytes_total,omitempty"` // full-copy phase only
	StartedAt  time.Time     `json:"started_at,omitzero"`
}

// Counters are the node's replication counters since process start
// (Applied/Failed/Blocked) plus current queue/backlog gauges.
type Counters struct {
	Applied      int64 `json:"applied_total"`
	Failed       int64 `json:"failed_total"`
	Blocked      int64 `json:"blocked_total"`
	OplogSize    int64 `json:"oplog_size"`
	PendingOps   int   `json:"pending_ops"`
	MissingBlobs int   `json:"missing_blobs"`
}

// ClusterStatus is a full point-in-time view of this node and the
// cluster as this node sees it — the Go equivalent of the superuser
// /api/replication/status endpoint.
type ClusterStatus struct {
	NodeID       string           `json:"node_id"`
	NodeURL      string           `json:"node_url"`
	Bootstrapped bool             `json:"bootstrapped"`
	HLC          string           `json:"hlc"`
	Members      []MemberInfo     `json:"members"`
	Vector       map[string]int64 `json:"vector"`
	Sync         SyncStatus       `json:"sync"`
	Counters     Counters         `json:"counters"`
	PeerLags     map[string]int64 `json:"peer_lags"`
	LastError    string           `json:"last_error,omitempty"`
}

// publishProgress makes the given bulk-sync state visible to the
// dashboard, /status and the exported SyncStatus accessor.
func (r *Replicator) publishProgress(s SyncStatus) {
	if s.ETA > 0 {
		s.ETAString = s.ETA.Round(time.Second).String()
	}
	r.progressState.Store(&s)
}

// clearProgress resets the live sync state to idle.
func (r *Replicator) clearProgress() {
	r.progressState.Store(&SyncStatus{Phase: SyncIdle})
}

// SyncStatus returns the live state of any bulk synchronization
// currently in progress (Phase == SyncIdle when none is).
func (r *Replicator) SyncStatus() SyncStatus {
	if v := r.progressState.Load(); v != nil {
		return *v
	}
	return SyncStatus{Phase: SyncIdle}
}

// Counters returns the node's replication counters and backlog gauges.
func (r *Replicator) Counters() Counters {
	var oplogSize sql.NullInt64
	_ = r.app.DB().NewQuery(`SELECT COUNT(*) FROM _repl_oplog`).Row(&oplogSize)
	return Counters{
		Applied:      r.stats.applied.Load(),
		Failed:       r.stats.failed.Load(),
		Blocked:      r.stats.blocked.Load(),
		OplogSize:    oplogSize.Int64,
		PendingOps:   r.pendingCount(),
		MissingBlobs: r.missingBlobCount(),
	}
}

// PeerLags returns, per peer, how many of THIS node's ops that peer has
// not acknowledged yet. Peers that never exchanged a vector with this
// node are omitted.
func (r *Replicator) PeerLags() map[string]int64 {
	out := map[string]int64{}
	r.peerVectors.Range(func(k, _ any) bool {
		id, _ := k.(string)
		if lag := r.peerLag(id); lag >= 0 {
			out[id] = lag
		}
		return true
	})
	return out
}

// LastError returns the most recent replication error message (empty
// when none occurred since process start).
func (r *Replicator) LastError() string {
	if v := r.stats.lastError.Load(); v != nil {
		s, _ := v.(string)
		return s
	}
	return ""
}

// Status assembles the full cluster view of this node — the Go
// equivalent of the superuser /api/replication/status endpoint.
func (r *Replicator) Status() ClusterStatus {
	vector, _ := r.currentVector()
	done, _ := getState(r.app.DB(), stateBootstrapDone)
	return ClusterStatus{
		NodeID:       r.nodeID,
		NodeURL:      r.cfg.NodeURL,
		Bootstrapped: done != "",
		HLC:          r.clock.Current(),
		Members:      r.Members(),
		Vector:       vector,
		Sync:         r.SyncStatus(),
		Counters:     r.Counters(),
		PeerLags:     r.PeerLags(),
		LastError:    r.LastError(),
	}
}
