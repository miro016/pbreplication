package pbreplication

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/security"
)

// Client side of the full database copy. A NEW node (no data.db yet)
// and a node flagged for resync (resync_pending state) replace their
// local database with a peer's snapshot BEFORE PocketBase opens it:
//
//	copy (pre-bootstrap) -> PocketBase opens the DB -> serve-time
//	migrations run only the files missing from the copied _migrations
//	table -> incremental sync pulls everything written since the
//	snapshot -> blobs backfill -> integrity check
//
// which is exactly the "full copy first, then only new migrations,
// then deltas" startup sequence. Local writes a resyncing node made
// while offline are rescued to a JSON file first and re-applied
// through the LWW gate after startup, so nothing is silently lost.

// errFullCopyUnsupported marks a seed too old to serve database
// snapshots; the caller falls back to the logical bootstrap.
var errFullCopyUnsupported = errors.New("peer does not support database snapshots")

type copyStrategy int

const (
	strategyNone copyStrategy = iota
	strategyFreshCopy
	strategyResyncCopy
)

func (r *Replicator) copyWorkDir() string {
	return filepath.Join(r.app.DataDir(), ".pbreplication")
}

func (r *Replicator) incomingDBPath() string {
	return filepath.Join(r.copyWorkDir(), "incoming.db")
}

func (r *Replicator) incomingSidecarPath() string {
	return filepath.Join(r.copyWorkDir(), "incoming.manifest.json")
}

func (r *Replicator) rescuePath() string {
	return filepath.Join(r.copyWorkDir(), "rescue.json")
}

// maybeFullCopyBootstrap runs BEFORE PocketBase opens its database
// (pre e.Next() in OnBootstrap). Any failure falls back to the legacy
// logical bootstrap instead of blocking startup.
func (r *Replicator) maybeFullCopyBootstrap(app core.App) error {
	if !*r.cfg.FullCopyBootstrap || r.cfg.SeedURL == "" {
		return nil
	}

	strategy, err := r.decideBootstrapStrategy(app)
	if err != nil {
		r.logError("full copy: strategy decision failed - continuing with normal startup", err)
		return nil
	}
	if strategy == strategyNone {
		return nil
	}

	if err := r.fullCopyBootstrap(app, strategy); err != nil {
		r.logError("full database copy failed - falling back to logical bootstrap", err)
		r.emitEvent(EventCopyFinished, "full database copy failed; falling back to logical sync",
			"peer", "", "error", err.Error())
		r.clearProgress()
	}
	return nil
}

// decideBootstrapStrategy inspects the (not yet opened) local database.
func (r *Replicator) decideBootstrapStrategy(app core.App) (copyStrategy, error) {
	dbPath := filepath.Join(app.DataDir(), "data.db")
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return strategyFreshCopy, nil
	} else if err != nil {
		return strategyNone, err
	}

	db, err := core.DefaultDBConnect(dbPath)
	if err != nil {
		return strategyNone, err
	}
	defer db.Close()

	// a plain PocketBase database without replication tables is handled
	// by the legacy logical bootstrap
	var hasState int
	if err := db.NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_repl_state'`).
		Row(&hasState); err != nil || hasState == 0 {
		return strategyNone, err
	}

	pending, err := getState(db, stateResyncPending)
	if err != nil {
		return strategyNone, err
	}
	if pending != "" {
		return strategyResyncCopy, nil
	}
	return strategyNone, nil
}

// fullCopyBootstrap retries the copy until it succeeds or the fallback
// budget (FullCopyFallbackAfter) is exhausted.
func (r *Replicator) fullCopyBootstrap(app core.App, strategy copyStrategy) error {
	ctx, cancel := context.WithTimeout(r.runCtx, r.cfg.FullCopyFallbackAfter)
	defer cancel()

	mode := "initial full copy (new node)"
	if strategy == strategyResyncCopy {
		mode = "full copy resync (node fell behind compaction)"
	}
	r.logMilestone("starting full database copy from seed", "seed", r.cfg.SeedURL, "mode", mode)

	var lastErr error
	for {
		if err := r.tryFullCopy(ctx, app, strategy); err == nil {
			return nil
		} else {
			lastErr = err
			if errors.Is(err, errFullCopyUnsupported) {
				return err
			}
			r.logWarn("full copy attempt failed (will retry)", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-r.stopCh:
			return lastErr
		case <-time.After(3 * time.Second):
		}
	}
}

func (r *Replicator) tryFullCopy(ctx context.Context, app core.App, strategy copyStrategy) error {
	seed := r.cfg.SeedURL

	// 1. manifest (a 404/405 means the seed predates this feature)
	mctx, mcancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	manifest := &dbSnapshotManifest{}
	err := r.callPeerCtx(mctx, seed, http.MethodPost, "/api/replication/snapshot/db", nil, manifest)
	mcancel()
	if err != nil {
		if s := httpStatus(err); s == http.StatusNotFound || s == http.StatusMethodNotAllowed {
			return fmt.Errorf("%w: %v", errFullCopyUnsupported, err)
		}
		return fmt.Errorf("fetch snapshot manifest: %w", err)
	}

	r.emitEvent(EventCopyStarted, "full database copy started",
		"peer", manifest.NodeID, "size_bytes", manifest.SizeBytes)

	// 2. download (resumes across restarts via the sidecar manifest)
	if err := r.downloadDBSnapshot(ctx, seed, manifest); err != nil {
		return err
	}

	// 3. node identity + offline-write rescue
	identity := rescueState{NodeID: r.cfg.NodeID}
	if identity.NodeID == "" {
		identity.NodeID = security.RandomString(15)
	}
	if strategy == strategyResyncCopy {
		rescued, err := r.rescueLocalState(filepath.Join(app.DataDir(), "data.db"), manifest)
		if err != nil {
			return fmt.Errorf("rescuing local unsynced writes: %w", err)
		}
		identity = *rescued
	}

	// 4. rewrite the copied database as THIS node
	if err := r.sanitizeCopiedDB(r.incomingDBPath(), manifest, &identity); err != nil {
		return fmt.Errorf("sanitizing copied database: %w", err)
	}

	// 5. install. Order matters for crash safety: unsynced local writes
	// are already persisted in rescue.json, so the old database (and its
	// WAL) is disposable from here on. The rename is atomic; stale WAL
	// files of the OLD database must not survive next to the NEW file.
	dbPath := filepath.Join(app.DataDir(), "data.db")
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
	if err := os.Rename(r.incomingDBPath(), dbPath); err != nil {
		return fmt.Errorf("installing copied database: %w", err)
	}
	_ = os.Remove(r.incomingSidecarPath())

	r.clearProgress()
	r.logMilestone("full database copy installed",
		"peer", manifest.NodeID, "size_mb", manifest.SizeBytes/(1<<20))
	r.emitEvent(EventCopyFinished, "full database copy installed",
		"peer", manifest.NodeID, "ok", true, "rescued_ops", len(identity.Ops))
	return nil
}

// downloadDBSnapshot fetches the snapshot file in chunks into
// incoming.db, resuming a previous partial download when the sidecar
// manifest matches, and verifies the full-file SHA-256 at the end.
func (r *Replicator) downloadDBSnapshot(ctx context.Context, baseURL string, m *dbSnapshotManifest) error {
	if err := os.MkdirAll(r.copyWorkDir(), 0o755); err != nil {
		return err
	}
	dest := r.incomingDBPath()
	sidecar := r.incomingSidecarPath()

	// resume only when the partial file belongs to the SAME snapshot
	offset := int64(0)
	if prev, err := os.ReadFile(sidecar); err == nil {
		var prevMan dbSnapshotManifest
		if json.Unmarshal(prev, &prevMan) == nil && prevMan.ID == m.ID && prevMan.SHA256 == m.SHA256 {
			if st, err := os.Stat(dest); err == nil {
				offset = st.Size()
			}
		}
	}
	if offset == 0 {
		if b, err := json.Marshal(m); err == nil {
			if err := os.WriteFile(sidecar, b, 0o600); err != nil {
				return err
			}
		}
		if err := os.WriteFile(dest, nil, 0o600); err != nil {
			return err
		}
	} else if offset > m.SizeBytes {
		offset = 0 // corrupt partial; restart
	} else {
		r.logMilestone("resuming interrupted database copy",
			"done_mb", offset/(1<<20), "total_mb", m.SizeBytes/(1<<20))
	}

	f, err := os.OpenFile(dest, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(offset); err != nil {
		return err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	chunk := int64(r.cfg.FullCopyChunkSize)
	start := time.Now()
	// generous per-chunk deadline so slow links still make progress,
	// while a stalled connection can't hang the copy forever
	chunkTimeout := 5 * r.cfg.RequestTimeout

	for offset < m.SizeBytes {
		var n int64
		err := r.withRetry(ctx, 4, time.Second, func() error {
			cctx, cancel := context.WithTimeout(ctx, chunkTimeout)
			defer cancel()

			path := fmt.Sprintf("/api/replication/snapshot/db/chunk?id=%s&offset=%d&limit=%d",
				m.ID, offset, chunk)
			rc, err := r.openPeerStreamCtx(cctx, baseURL, path, 0)
			if err != nil {
				if s := httpStatus(err); s == http.StatusNotFound || s == http.StatusBadRequest {
					// snapshot expired on the peer - restart from scratch
					return fmt.Errorf("%w: snapshot gone: %v", errPermanent, err)
				}
				return err
			}
			defer rc.Close()

			n, err = io.Copy(f, io.LimitReader(rc, chunk))
			return err
		})
		if err != nil {
			if errors.Is(err, errPermanent) {
				_ = os.Remove(dest)
				_ = os.Remove(sidecar)
			}
			return fmt.Errorf("chunk at offset %d: %w", offset, err)
		}
		if n == 0 {
			return fmt.Errorf("chunk at offset %d: empty response", offset)
		}
		offset += n

		elapsed := time.Since(start).Seconds()
		rate := float64(offset) / (1 << 20) / max(elapsed, 0.001)
		st := SyncStatus{
			Phase:      SyncCopying,
			Peer:       m.NodeID,
			BytesDone:  offset,
			BytesTotal: m.SizeBytes,
			Percent:    int(offset * 100 / max(m.SizeBytes, 1)),
			StartedAt:  start,
		}
		if offset < m.SizeBytes && rate > 0 {
			st.ETA = time.Duration(float64(m.SizeBytes-offset) / (rate * (1 << 20)) * float64(time.Second))
		}
		r.publishProgress(st)
		r.consoleProgress("copying database: %d/%d MB (%d%%) %.1f MB/s",
			offset/(1<<20), m.SizeBytes/(1<<20), st.Percent, rate)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	r.consoleProgressDone("database copy complete: %d MB in %s",
		m.SizeBytes/(1<<20), time.Since(start).Round(time.Second))

	// full-file integrity check against the HMAC-authenticated manifest
	vf, err := os.Open(dest)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(h, vf)
	vf.Close()
	if err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != m.SHA256 {
		_ = os.Remove(dest)
		_ = os.Remove(sidecar)
		return fmt.Errorf("checksum mismatch (got %s, want %s)", got, m.SHA256)
	}
	return nil
}

// rescueState carries a resyncing node's identity plus every local op
// the cluster has not acknowledged yet. It is persisted to rescue.json
// BEFORE the local database is replaced, and replayed through the LWW
// gate after startup.
type rescueState struct {
	NodeID   string `json:"node_id"`
	LocalSeq int64  `json:"local_seq"`
	HLC      string `json:"hlc"`
	Ops      []*op  `json:"ops"`
}

// rescueLocalState extracts identity and unacknowledged ops from the
// old database and persists them to rescue.json.
func (r *Replicator) rescueLocalState(dbPath string, m *dbSnapshotManifest) (*rescueState, error) {
	db, err := core.DefaultDBConnect(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rescue := &rescueState{}
	if rescue.NodeID, err = getState(db, stateNodeID); err != nil {
		return nil, err
	}
	if rescue.NodeID == "" {
		return nil, errors.New("old database has no node identity")
	}
	if v, _ := getState(db, stateLocalSeq); v != "" {
		fmt.Sscanf(v, "%d", &rescue.LocalSeq)
	}
	rescue.HLC, _ = getState(db, stateHLC)

	// everything the seed provably has from us is covered by its vector
	ack := m.Vector[rescue.NodeID]
	var rows []oplogRow
	if err := db.NewQuery(`SELECT rowid, * FROM _repl_oplog
		WHERE src_node = {:s} AND src_seq > {:a} ORDER BY src_seq`).
		Bind(dbx.Params{"s": rescue.NodeID, "a": ack}).All(&rows); err != nil {
		return nil, err
	}
	for i := range rows {
		rescue.Ops = append(rescue.Ops, rows[i].toOp())
	}

	if err := os.MkdirAll(r.copyWorkDir(), 0o755); err != nil {
		return nil, err
	}
	b, err := json.Marshal(rescue)
	if err != nil {
		return nil, err
	}
	tmp := r.rescuePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, r.rescuePath()); err != nil {
		return nil, err
	}

	r.logMilestone("rescued unsynced local writes before resync",
		"ops", len(rescue.Ops), "acked_by_seed", ack)
	return rescue, nil
}

// sanitizeCopiedDB rewrites the freshly downloaded database so it
// belongs to THIS node: identity, vector/sequence bookkeeping,
// membership self-row, and node-local data that must not travel.
func (r *Replicator) sanitizeCopiedDB(path string, m *dbSnapshotManifest, identity *rescueState) error {
	db, err := core.DefaultDBConnect(path)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Transactional(func(tx *dbx.Tx) error {
		// our local_seq must clear (a) anything the cluster already has
		// from a previous life of this node id and (b) any of our ops
		// physically present in the copied oplog, so re-issued sequence
		// numbers can never collide
		localSeq := identity.LocalSeq
		if v := m.Vector[identity.NodeID]; v > localSeq {
			localSeq = v
		}
		var maxOwn int64
		_ = tx.NewQuery(`SELECT COALESCE(MAX(src_seq), 0) FROM _repl_oplog WHERE src_node = {:s}`).
			Bind(dbx.Params{"s": identity.NodeID}).Row(&maxOwn)
		if maxOwn > localSeq {
			localSeq = maxOwn
		}

		// the copy's clock is the peer's; keep whichever is further ahead
		hlcVal, _ := getState(tx, stateHLC)
		if identity.HLC > hlcVal {
			hlcVal = identity.HLC
		}
		if m.HLC > hlcVal {
			hlcVal = m.HLC
		}

		states := map[string]string{
			stateNodeID:              identity.NodeID,
			stateLocalSeq:            fmt.Sprintf("%d", localSeq),
			stateHLC:                 hlcVal,
			stateBootstrapDone:       nowStr(),
			stateResyncPending:       "",
			stateSnapshotResume:      "",
			stateBlobBackfillPending: "1",
			// the peer's own ops are covered up to its manifest seq
			stateVectorPrefix + m.NodeID: fmt.Sprintf("%d", m.Vector[m.NodeID]),
		}
		for k, v := range states {
			if err := setState(tx, k, v); err != nil {
				return err
			}
		}
		// we don't keep a vector entry for ourselves (local_seq covers it)
		if _, err := tx.NewQuery(`DELETE FROM _repl_state WHERE key = {:k}`).
			Bind(dbx.Params{"k": stateVectorPrefix + identity.NodeID}).Execute(); err != nil {
			return err
		}

		// membership: make sure our own row exists (the URL is set
		// authoritatively in startBackground)
		if err := upsertMember(tx, &member{
			NodeID:    identity.NodeID,
			URL:       r.cfg.NodeURL,
			Reachable: r.cfg.NodeURL != "",
			LastSeen:  nowStr(),
		}); err != nil {
			return err
		}

		// node-local telemetry must not carry over
		for _, table := range []string{"_repl_client_ips", "_repl_client_paths", "_repl_sync_seen"} {
			if _, err := tx.NewQuery(`DELETE FROM ` + table).Execute(); err != nil {
				return err
			}
		}

		// excluded collections were physically copied with the file -
		// empty them (the schema stays; that matches the logical path
		// where excluded collections simply never sync)
		for name := range r.excluded {
			var exists int
			if err := tx.NewQuery(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = {:n}`).
				Bind(dbx.Params{"n": name}).Row(&exists); err != nil || exists == 0 {
				continue
			}
			if _, err := tx.NewQuery(`DELETE FROM {{` + name + `}}`).Execute(); err != nil {
				return err
			}
		}
		return nil
	})
}

// replayRescuedOps re-applies the rescued unsynced local writes through
// the LWW gate (with their ORIGINAL HLCs - a later cluster write must
// still win) and re-emits them into the oplog with fresh sequence
// numbers so they replicate out to the cluster. Idempotent: replays of
// ops the cluster already knows are LWW no-ops there.
func (r *Replicator) replayRescuedOps() {
	raw, err := os.ReadFile(r.rescuePath())
	if err != nil {
		return // nothing to replay
	}
	var rescue rescueState
	if err := json.Unmarshal(raw, &rescue); err != nil {
		r.logError("rescue file unreadable (skipping replay)", err)
		_ = os.Rename(r.rescuePath(), r.rescuePath()+".corrupt")
		return
	}
	if rescue.NodeID != r.nodeID {
		r.logWarn("rescue file belongs to a different node id - skipping replay",
			"rescued", rescue.NodeID, "self", r.nodeID)
		_ = os.Remove(r.rescuePath())
		return
	}

	replayed, skipped := 0, 0
	for _, o := range rescue.Ops {
		ok, err := r.replayRescuedOp(o)
		if err != nil {
			r.logError("replay rescued op "+o.ColName+"/"+o.RecordID, err, "collection", o.ColName)
			continue
		}
		if ok {
			replayed++
		} else {
			skipped++
		}
	}

	_ = os.Remove(r.rescuePath())
	if len(rescue.Ops) > 0 {
		r.logMilestone("re-applied offline writes after full copy",
			"replayed", replayed, "superseded_by_cluster", skipped)
		r.emitEvent(EventCopyFinished, "offline writes re-applied after full copy",
			"replayed", replayed, "superseded", skipped)
	}
	wake(r.pushWake)
}

// replayRescuedOp applies one rescued op if it still wins LWW, and
// re-emits it into the oplog under a fresh local sequence number with
// its original HLC. Returns whether the op was applied.
func (r *Replicator) replayRescuedOp(o *op) (bool, error) {
	switch o.Type {
	case opColUpsert, opColDelete:
		// applyCollectionOp is LWW-gated and marked internally; re-emit
		// only when it would supersede
		cur, err := getVersion(r.app.DB(), collectionsColID, o.RecordID)
		if err != nil {
			return false, err
		}
		if !supersedes(o, cur) {
			return false, nil
		}
		if err := r.applyCollectionOp(o); err != nil {
			return false, err
		}
		return true, r.app.RunInTransaction(func(txApp core.App) error {
			db := txApp.NonconcurrentDB()
			seq, err := incrLocalSeq(db)
			if err != nil {
				return err
			}
			emitted := *o
			emitted.SrcSeq = seq
			return insertOp(db, &emitted)
		})

	case opUpsert, opDelete:
		cur, err := getVersion(r.app.DB(), o.ColID, o.RecordID)
		if err != nil {
			return false, err
		}
		if !supersedes(o, cur) {
			return false, nil
		}
		col := r.resolveCollection(o)
		if col == nil {
			return false, fmt.Errorf("collection %s not found", o.ColName)
		}
		if o.Type == opUpsert && len(o.Files) > 0 {
			r.fetchFilesForOp(o, col)
		}

		ctx := markedCtx(context.Background(), o)
		applied := false
		err = r.app.RunInTransaction(func(txApp core.App) error {
			db := txApp.NonconcurrentDB()
			cur, err := getVersion(db, o.ColID, o.RecordID)
			if err != nil {
				return err
			}
			if !supersedes(o, cur) {
				return nil
			}

			switch o.Type {
			case opUpsert:
				rec, err := txApp.FindRecordById(col, o.RecordID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if rec == nil {
					rec = core.NewRecord(col)
					rec.Id = o.RecordID
				}
				if err := applyPayload(rec, o.Payload); err != nil {
					return err
				}
				if err := txApp.SaveNoValidateWithContext(ctx, rec); err != nil {
					return err
				}
			case opDelete:
				rec, err := txApp.FindRecordById(col, o.RecordID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if rec != nil {
					if err := txApp.DeleteWithContext(ctx, rec); err != nil {
						return err
					}
				}
			}

			seq, err := incrLocalSeq(db)
			if err != nil {
				return err
			}
			emitted := *o
			emitted.SrcSeq = seq
			if err := insertOp(db, &emitted); err != nil {
				return err
			}
			if err := upsertVersion(db, o.ColID, o.RecordID, o.HLC, o.SrcNode, o.Type == opDelete); err != nil {
				return err
			}
			applied = true
			return nil
		})
		return applied, err
	}
	return false, fmt.Errorf("unknown op type %q", o.Type)
}
