package pbreplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/pocketbase/pocketbase/core"
)

// applyLoop is the single goroutine that applies remote ops. Serializing
// applies keeps LWW bookkeeping simple and matches SQLite's single-writer
// model anyway.
func (r *Replicator) applyLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopCh:
			return
		case o := <-r.applyCh:
			if err := r.applyOp(o); err != nil {
				r.stats.failed.Add(1)
				r.logError(fmt.Sprintf("apply %s %s/%s (from %s#%d)", o.Type, o.ColName, o.RecordID, o.SrcNode, o.SrcSeq), err)
			}
		}
	}
}

// enqueueApply hands an op to the applier, blocking if the queue is
// full (natural backpressure for ingest).
func (r *Replicator) enqueueApply(o *op) {
	select {
	case r.applyCh <- o:
	case <-r.stopCh:
	}
}

func (r *Replicator) applyOp(o *op) error {
	r.clock.Observe(o.HLC)

	if o.SrcNode == r.nodeID {
		return nil // own op echoed back through gossip
	}

	switch o.Type {
	case opColUpsert, opColDelete:
		return r.applyCollectionOp(o)
	case opUpsert, opDelete:
		return r.applyRecordOp(o)
	default:
		return fmt.Errorf("unknown op type %q", o.Type)
	}
}

// supersedes reports whether the op should be applied given the current
// LWW version row (nil row = never seen locally).
func supersedes(o *op, cur *versionRow) bool {
	if cur == nil {
		return true
	}
	return lwwLess(cur.HLC, cur.SrcNode, o.HLC, o.SrcNode)
}

// ---------------------------------------------------------------------
// record ops

func (r *Replicator) applyRecordOp(o *op) error {
	// cheap pre-check outside the tx
	cur, err := getVersion(r.app.DB(), o.ColID, o.RecordID)
	if err != nil {
		return err
	}
	if !supersedes(o, cur) {
		return nil
	}

	col := r.resolveCollection(o)
	if col == nil {
		r.parkPending(o)
		return nil
	}
	if !r.isReplicated(col) {
		return nil
	}

	// fetch any missing file blobs BEFORE the write transaction
	if o.Type == opUpsert && len(o.Files) > 0 {
		r.fetchFilesForOp(o, col)
	}

	ctx := markedCtx(context.Background(), o)

	return r.app.RunInTransaction(func(txApp core.App) error {
		db := txApp.NonconcurrentDB()

		// authoritative LWW gate inside the tx
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

		if err := upsertVersion(db, o.ColID, o.RecordID, o.HLC, o.SrcNode, o.Type == opDelete); err != nil {
			return err
		}

		r.stats.applied.Add(1)
		return nil
	})
}

// resolveCollection finds the local collection for an op, by id first
// and by name as a fallback (covers the "same collection created
// independently on two nodes with different random ids" case).
func (r *Replicator) resolveCollection(o *op) *core.Collection {
	if col, err := r.app.FindCachedCollectionByNameOrId(o.ColID); err == nil && col != nil {
		return col
	}
	if o.ColName != "" {
		if col, err := r.app.FindCachedCollectionByNameOrId(o.ColName); err == nil && col != nil {
			return col
		}
	}
	return nil
}

// parkPending stores an op whose collection isn't known locally yet.
// Parked ops are retried after every sync round (the schema op usually
// arrives moments later).
func (r *Replicator) parkPending(o *op) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if len(r.pendingOps) >= maxPendingOps {
		r.pendingOps = r.pendingOps[1:] // drop oldest; anti-entropy re-delivers
	}
	r.pendingOps = append(r.pendingOps, o)
}

// retryPending re-enqueues all parked ops (called after sync rounds and
// after collection ops were applied). It never blocks: when the apply
// queue is full the op is simply parked again for the next round. This
// matters because retryPending can run on the applier goroutine itself
// (after a collection op) - a blocking send there would self-deadlock.
func (r *Replicator) retryPending() {
	r.pendingMu.Lock()
	parked := r.pendingOps
	r.pendingOps = nil
	r.pendingMu.Unlock()

	for _, o := range parked {
		// only retry if the collection is resolvable now, otherwise park again
		if r.resolveCollection(o) == nil {
			r.parkPending(o)
			continue
		}
		select {
		case r.applyCh <- o:
		default:
			r.parkPending(o)
		}
	}
}

func (r *Replicator) pendingCount() int {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	return len(r.pendingOps)
}

// ---------------------------------------------------------------------
// collection (schema) ops

// canonicalCollectionJSON marshals a collection to its canonical wire
// form (used for the content-hash idempotence check). It must match the
// capture-side serialization byte for byte.
func canonicalCollectionJSON(col *core.Collection) ([]byte, error) {
	return exportCollectionJSON(col)
}

func contentHash(b []byte) [32]byte {
	return sha256.Sum256(b)
}

func (r *Replicator) applyCollectionOp(o *op) error {
	cur, err := getVersion(r.app.DB(), collectionsColID, o.RecordID)
	if err != nil {
		return err
	}

	existing, err := r.app.FindCollectionByNameOrId(o.ColID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if existing == nil && o.ColName != "" {
		existing, err = r.app.FindCollectionByNameOrId(o.ColName)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}

	ctx := markedCtx(context.Background(), o)

	switch o.Type {
	case opColUpsert:
		// Idempotence: the same migration executed on N nodes produces
		// byte-identical collection JSON -> only bump the version row.
		if existing != nil {
			local, err := canonicalCollectionJSON(existing)
			if err == nil && contentHash(local) == contentHash(bytes.TrimSpace(o.Payload)) {
				if supersedes(o, cur) {
					return upsertVersion(r.app.NonconcurrentDB(), collectionsColID, o.RecordID, o.HLC, o.SrcNode, false)
				}
				return nil
			}
		}

		if !supersedes(o, cur) {
			return nil
		}

		var col *core.Collection
		if existing != nil {
			// refetch for a deep copy, then unmarshal the imported data
			// on top (mirrors core.ImportCollections normalization)
			col, err = r.app.FindCollectionByNameOrId(existing.Id)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(o.Payload, col); err != nil {
				return err
			}
			// preserve existing field ids to prevent accidental column
			// drops when the remote used different random field ids
			for _, f := range existing.Fields {
				if col.Fields.GetById(f.GetId()) == nil {
					found := col.Fields.GetByName(f.GetName())
					if found != nil && found.Type() == f.Type() {
						found.SetId(f.GetId())
					} else if f.GetSystem() {
						col.Fields.Add(f)
					}
				}
			}
		} else {
			col = &core.Collection{}
			if err := json.Unmarshal(o.Payload, col); err != nil {
				return err
			}
		}
		col.IntegrityChecks(false)

		if err := r.app.SaveNoValidateWithContext(ctx, col); err != nil {
			return err
		}
		if err := upsertVersion(r.app.NonconcurrentDB(), collectionsColID, o.RecordID, o.HLC, o.SrcNode, false); err != nil {
			return err
		}
		r.stats.applied.Add(1)
		r.retryPending() // parked record ops may be resolvable now
		return nil

	case opColDelete:
		if !supersedes(o, cur) {
			return nil
		}
		if existing != nil {
			if err := r.app.DeleteWithContext(ctx, existing); err != nil {
				return err
			}
		}
		if err := upsertVersion(r.app.NonconcurrentDB(), collectionsColID, o.RecordID, o.HLC, o.SrcNode, true); err != nil {
			return err
		}
		r.stats.applied.Add(1)
		return nil
	}

	return nil
}
