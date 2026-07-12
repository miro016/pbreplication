package pbreplication

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/security"
)

// Server side of the full database copy: a consistent point-in-time
// SQLite snapshot produced with VACUUM INTO, advertised via a manifest
// and served in chunks so downloads survive flaky links and resume
// after interruptions.

// dbSnapshotManifest describes one prepared database snapshot file.
type dbSnapshotManifest struct {
	ID        string           `json:"id"`
	NodeID    string           `json:"node_id"`
	SizeBytes int64            `json:"size_bytes"`
	SHA256    string           `json:"sha256"`
	// Vector is the serving node's contiguous vector INCLUDING its own
	// local sequence, read BEFORE the vacuum started. Understating is
	// safe: ops the snapshot already contains replay as LWW no-ops.
	Vector    map[string]int64 `json:"vector"`
	HLC       string           `json:"hlc"`
	Members   []*member        `json:"members,omitempty"`
	CreatedAt string           `json:"created_at"`
}

// dbSnapshotDir is where prepared snapshot files live, relative to the
// PocketBase data dir.
const dbSnapshotDir = ".pbreplication/snapshots"

func (r *Replicator) dbSnapshotPath(id string) string {
	return filepath.Join(r.app.DataDir(), dbSnapshotDir, id+".db")
}

// prepareDBSnapshot returns a manifest for a ready-to-download snapshot
// file, reusing a cached one younger than SnapshotCacheTTL so N joiners
// share one vacuum and Range-style resumes target immutable bytes.
func (r *Replicator) prepareDBSnapshot() (*dbSnapshotManifest, error) {
	r.dbSnapMu.Lock()
	defer r.dbSnapMu.Unlock()

	if r.dbSnapManifest != nil && time.Since(r.dbSnapCreated) < r.cfg.SnapshotCacheTTL {
		if _, err := os.Stat(r.dbSnapshotPath(r.dbSnapManifest.ID)); err == nil {
			return r.dbSnapManifest, nil
		}
	}

	// vector BEFORE the vacuum (see manifest field doc)
	vector, err := r.currentVector()
	if err != nil {
		return nil, err
	}
	members, _ := listMembers(r.app.DB(), false)

	id := security.RandomString(16)
	path := r.dbSnapshotPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	start := time.Now()
	// VACUUM INTO produces a consistent, WAL-free copy without blocking
	// readers; run on the concurrent pool so normal writes queue only
	// behind SQLite's own locking.
	if _, err := r.app.DB().NewQuery("VACUUM INTO {:p}").
		Bind(dbx.Params{"p": path}).Execute(); err != nil {
		return nil, fmt.Errorf("vacuum into snapshot: %w", err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		os.Remove(path)
		return nil, err
	}

	// drop the previously cached snapshot file
	if r.dbSnapManifest != nil {
		os.Remove(r.dbSnapshotPath(r.dbSnapManifest.ID))
	}

	r.dbSnapManifest = &dbSnapshotManifest{
		ID:        id,
		NodeID:    r.nodeID,
		SizeBytes: size,
		SHA256:    hex.EncodeToString(h.Sum(nil)),
		Vector:    vector,
		HLC:       r.clock.Current(),
		Members:   members,
		CreatedAt: nowStr(),
	}
	r.dbSnapCreated = time.Now()

	r.logMilestone("database snapshot prepared for a joining node",
		"size_mb", size/(1<<20), "took", time.Since(start).Round(time.Millisecond).String())
	return r.dbSnapManifest, nil
}

// handleDBSnapshotPrepare (POST /api/replication/snapshot/db) prepares
// (or reuses) a snapshot and returns its manifest.
func (r *Replicator) handleDBSnapshotPrepare(e *core.RequestEvent) error {
	m, err := r.prepareDBSnapshot()
	if err != nil {
		r.logError("prepare db snapshot", err)
		return e.InternalServerError("failed to prepare snapshot", nil)
	}
	return e.JSON(http.StatusOK, m)
}

// handleDBSnapshotChunk (GET /api/replication/snapshot/db/chunk)
// serves a byte range of a prepared snapshot file. Offsets always refer
// to the uncompressed file; each chunk response is independently
// gzipped when the client accepts it, which keeps resume and
// compression compatible.
func (r *Replicator) handleDBSnapshotChunk(e *core.RequestEvent) error {
	q := e.Request.URL.Query()
	id := q.Get("id")
	// ids come from security.RandomString - reject anything path-like
	if id == "" || id != filepath.Base(id) || strings.ContainsAny(id, "./\\") {
		return e.BadRequestError("invalid snapshot id", nil)
	}
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	limit, _ := strconv.ParseInt(q.Get("limit"), 10, 64)
	if offset < 0 || limit <= 0 {
		return e.BadRequestError("invalid range", nil)
	}

	f, err := os.Open(r.dbSnapshotPath(id))
	if err != nil {
		return e.NotFoundError("unknown snapshot (expired or never prepared)", nil)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return e.InternalServerError("stat snapshot", nil)
	}
	if offset >= st.Size() {
		return e.BadRequestError("offset beyond snapshot end", nil)
	}
	if offset+limit > st.Size() {
		limit = st.Size() - offset
	}

	src := io.NewSectionReader(f, offset, limit)
	w := e.Response

	if strings.Contains(e.Request.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		gz := gzip.NewWriter(w)
		if _, err := io.Copy(gz, src); err != nil {
			return err
		}
		return gz.Close()
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(limit, 10))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, src)
	return err
}

// cleanupDBSnapshots removes prepared snapshot files that are past
// twice the cache TTL (nothing can legitimately still download them:
// clients re-fetch the manifest when their snapshot id disappears).
func (r *Replicator) cleanupDBSnapshots() {
	dir := filepath.Join(r.app.DataDir(), dbSnapshotDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-2 * r.cfg.SnapshotCacheTTL)
	for _, ent := range entries {
		info, err := ent.Info()
		if err != nil || info.IsDir() {
			continue
		}
		if info.ModTime().Before(cutoff) {
			r.dbSnapMu.Lock()
			cachedID := ""
			if r.dbSnapManifest != nil {
				cachedID = r.dbSnapManifest.ID
			}
			if ent.Name() != cachedID+".db" {
				os.Remove(filepath.Join(dir, ent.Name()))
			}
			r.dbSnapMu.Unlock()
		}
	}
}
