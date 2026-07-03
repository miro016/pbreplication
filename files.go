package pbreplication

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

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
