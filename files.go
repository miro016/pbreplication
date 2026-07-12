package pbreplication

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/filesystem"
)

// missingBlob describes a file that is referenced by a replicated
// record but whose bytes could not be fetched from any peer yet.
type missingBlob struct {
	ColName  string
	RecordID string
	Name     string
	LocalKey string
}

const maxMissingBlobs = 10000

// fetchFilesForOp downloads any file blobs referenced by the op that
// are not present in local storage yet. Failures are non-fatal: the
// record is still applied and the blob is retried after every sync
// round (any node that already applied the op can serve it).
func (r *Replicator) fetchFilesForOp(o *op, col *core.Collection) {
	fsys, err := r.app.NewFilesystem()
	if err != nil {
		r.logError("files: open filesystem", err)
		return
	}
	defer fsys.Close()

	for _, names := range o.Files {
		for _, name := range names {
			localKey := col.Id + "/" + o.RecordID + "/" + name
			if ok, _ := fsys.Exists(localKey); ok {
				continue
			}
			if err := r.fetchBlob(fsys, o.ColName, o.RecordID, name, localKey, o.SrcNode); err != nil {
				r.parkMissingBlob(&missingBlob{
					ColName: o.ColName, RecordID: o.RecordID, Name: name, LocalKey: localKey,
				})
			}
		}
	}
}

// fetchBlob tries the op's source node first, then every other healthy
// reachable member, and installs the blob under localKey.
func (r *Replicator) fetchBlob(fsys *filesystem.System, colName, recordID, name, localKey, preferNode string) error {
	path := fmt.Sprintf("/api/replication/file/%s/%s/%s",
		url.PathEscape(colName), url.PathEscape(recordID), url.PathEscape(name))

	members, err := listMembers(r.app.DB(), false)
	if err != nil {
		return err
	}

	// preferred source first
	ordered := make([]*member, 0, len(members))
	for _, m := range members {
		if m.NodeID == preferNode && m.URL != "" {
			ordered = append(ordered, m)
		}
	}
	for _, m := range members {
		if m.NodeID != preferNode && m.NodeID != r.nodeID && m.URL != "" {
			ordered = append(ordered, m)
		}
	}

	var lastErr error = fmt.Errorf("no reachable peers")
	for _, m := range ordered {
		rc, err := r.openPeerStream(r.peerURL(m), path)
		if err != nil {
			lastErr = err
			continue
		}
		err = r.installBlob(fsys, rc, name, localKey)
		rc.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// installBlob spools the stream to a temp file (bounded memory) and
// uploads it into the storage backend under the exact key the record
// references.
func (r *Replicator) installBlob(fsys *filesystem.System, src io.Reader, name, localKey string) error {
	tmp, err := os.CreateTemp("", "pbr_blob_*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	f, err := filesystem.NewFileFromPath(tmpPath)
	if err != nil {
		return err
	}
	f.Name = name
	f.OriginalName = name

	return fsys.UploadFile(f, localKey)
}

// serveBlob streams a locally stored record file to a peer.
func (r *Replicator) serveBlob(e *core.RequestEvent, colNameOrID, recordID, name string) error {
	// path traversal guard
	for _, part := range []string{colNameOrID, recordID, name} {
		if part == "" || part != filepath.Base(part) || part == "." || part == ".." {
			return e.BadRequestError("invalid file path", nil)
		}
	}

	col, err := r.app.FindCachedCollectionByNameOrId(colNameOrID)
	if err != nil || col == nil {
		return e.NotFoundError("unknown collection", nil)
	}

	fsys, err := r.app.NewFilesystem()
	if err != nil {
		return e.InternalServerError("filesystem unavailable", nil)
	}
	defer fsys.Close()

	key := col.Id + "/" + recordID + "/" + name
	reader, err := fsys.GetReader(key)
	if err != nil {
		return e.NotFoundError("file not found", nil)
	}
	defer reader.Close()

	return e.Stream(200, "application/octet-stream", reader)
}

// ---------------------------------------------------------------------
// missing blob retry list

func (r *Replicator) parkMissingBlob(b *missingBlob) {
	r.blobMu.Lock()
	defer r.blobMu.Unlock()
	if len(r.missingBlobList) >= maxMissingBlobs {
		r.missingBlobList = r.missingBlobList[1:]
	}
	r.missingBlobList = append(r.missingBlobList, b)
}

// retryMissingBlobs re-attempts fetching blobs that were unavailable.
func (r *Replicator) retryMissingBlobs() {
	r.blobMu.Lock()
	blobs := r.missingBlobList
	r.missingBlobList = nil
	r.blobMu.Unlock()

	if len(blobs) == 0 {
		return
	}

	fsys, err := r.app.NewFilesystem()
	if err != nil {
		r.blobMu.Lock()
		r.missingBlobList = blobs
		r.blobMu.Unlock()
		return
	}
	defer fsys.Close()

	for _, b := range blobs {
		if ok, _ := fsys.Exists(b.LocalKey); ok {
			continue
		}
		if err := r.fetchBlob(fsys, b.ColName, b.RecordID, b.Name, b.LocalKey, ""); err != nil {
			r.parkMissingBlob(b)
		}
	}
}

func (r *Replicator) missingBlobCount() int {
	r.blobMu.Lock()
	defer r.blobMu.Unlock()
	return len(r.missingBlobList)
}

// ---------------------------------------------------------------------
// blob backfill after a full database copy
//
// A copied database references files whose bytes never traveled with
// it. While the persistent blob_backfill_pending flag is set, sync
// rounds run backfill passes: walk every replicated collection with
// file fields (keyset-paged), fetch what's missing, and clear the flag
// only after a pass completes with nothing left to fetch - so the
// backfill also survives restarts.

// maybeBackfillBlobs starts an async backfill pass when one is due.
func (r *Replicator) maybeBackfillBlobs() {
	pending, err := getState(r.app.DB(), stateBlobBackfillPending)
	if err != nil || pending == "" {
		return
	}
	// passes rescan every record with file fields - keep them spaced
	if !r.throttleOK("blob_backfill", 2*r.cfg.SyncInterval) {
		return
	}
	if !r.blobBackfillInFlight.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer r.blobBackfillInFlight.Store(false)
		clean, err := r.backfillBlobs()
		if err != nil {
			r.logError("blob backfill pass failed", err)
			return
		}
		if clean {
			_ = setState(r.app.NonconcurrentDB(), stateBlobBackfillPending, "")
			r.logMilestone("file blob backfill complete")
		}
	}()
}

// backfillBlobs runs one pass over all file-field records. It returns
// true when nothing was missing anymore (the pass was clean).
func (r *Replicator) backfillBlobs() (bool, error) {
	fsys, err := r.app.NewFilesystem()
	if err != nil {
		return false, err
	}
	defer fsys.Close()

	cols, err := r.app.FindAllCollections()
	if err != nil {
		return false, err
	}

	published := false
	if r.SyncStatus().Phase == SyncIdle {
		r.publishProgress(SyncStatus{Phase: SyncBlobBackfill, StartedAt: time.Now()})
		published = true
	}
	if published {
		defer r.clearProgress()
	}

	clean := true
	checked := 0
	for _, col := range cols {
		if !r.isReplicated(col) {
			continue
		}
		var fileFields []string
		for _, f := range col.Fields {
			if f.Type() == core.FieldTypeFile {
				fileFields = append(fileFields, f.GetName())
			}
		}
		if len(fileFields) == 0 {
			continue
		}

		after := ""
		for {
			records := []*core.Record{}
			err := r.app.RecordQuery(col).
				AndWhere(dbx.NewExp("id > {:after}", dbx.Params{"after": after})).
				OrderBy("id ASC").Limit(500).All(&records)
			if err != nil {
				return false, err
			}
			if len(records) == 0 {
				break
			}
			after = records[len(records)-1].Id

			for _, rec := range records {
				for _, field := range fileFields {
					for _, name := range rec.GetStringSlice(field) {
						checked++
						localKey := col.Id + "/" + rec.Id + "/" + name
						if ok, _ := fsys.Exists(localKey); ok {
							continue
						}
						if err := r.fetchBlob(fsys, col.Name, rec.Id, name, localKey, ""); err != nil {
							clean = false
							r.parkMissingBlob(&missingBlob{
								ColName: col.Name, RecordID: rec.Id, Name: name, LocalKey: localKey,
							})
						}
					}
				}
			}

			select {
			case <-r.stopCh:
				return false, nil
			default:
			}
			if len(records) < 500 {
				break
			}
		}
	}
	r.logInfo("blob backfill pass finished", "files_checked", checked, "clean", clean)
	return clean, nil
}
