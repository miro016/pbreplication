package pbreplication

import (
	"context"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
)

// replCtxKey marks a context as belonging to an op that is being
// applied from a remote node, so the capture hooks don't re-log it
// (loop prevention).
type replCtxKey struct{}

func markedCtx(parent context.Context, o *op) context.Context {
	return context.WithValue(parent, replCtxKey{}, o)
}

func isMarked(ctx context.Context) bool {
	return ctx != nil && ctx.Value(replCtxKey{}) != nil
}

// hookPriority runs the capture hooks after user hooks so replicated
// payloads include any mutations user code made during the save.
const hookPriority = 999

// bindCaptureHooks registers the hooks that record local changes into
// the oplog. Record capture starts or reuses a write transaction;
// collection capture runs inside PocketBase's schema transaction.
func (r *Replicator) bindCaptureHooks(app core.App) {
	app.OnRecordCreateExecute().Bind(&hook.Handler[*core.RecordEvent]{
		Id: "pbreplicationCaptureCreate", Priority: hookPriority, Func: r.captureRecord(opUpsert),
	})
	app.OnRecordUpdateExecute().Bind(&hook.Handler[*core.RecordEvent]{
		Id: "pbreplicationCaptureUpdate", Priority: hookPriority, Func: r.captureRecord(opUpsert),
	})
	app.OnRecordDeleteExecute().Bind(&hook.Handler[*core.RecordEvent]{
		Id: "pbreplicationCaptureDelete", Priority: hookPriority, Func: r.captureRecord(opDelete),
	})

	// post-commit: wake the pusher
	wakeFn := func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		if r.ready.Load() && r.isReplicated(e.Record.Collection()) {
			wake(r.pushWake)
		}
		return nil
	}
	app.OnRecordAfterCreateSuccess().BindFunc(wakeFn)
	app.OnRecordAfterUpdateSuccess().BindFunc(wakeFn)
	app.OnRecordAfterDeleteSuccess().BindFunc(wakeFn)

	// collection (schema) changes
	app.OnCollectionCreateExecute().Bind(&hook.Handler[*core.CollectionEvent]{
		Id: "pbreplicationCaptureCollectionCreate", Priority: hookPriority, Func: r.captureCollection(opColUpsert),
	})
	app.OnCollectionUpdateExecute().Bind(&hook.Handler[*core.CollectionEvent]{
		Id: "pbreplicationCaptureCollectionUpdate", Priority: hookPriority, Func: r.captureCollection(opColUpsert),
	})
	app.OnCollectionDeleteExecute().Bind(&hook.Handler[*core.CollectionEvent]{
		Id: "pbreplicationCaptureCollectionDelete", Priority: hookPriority, Func: r.captureCollection(opColDelete),
	})
	wakeCollectionFn := func(e *core.CollectionEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		if r.ready.Load() && !isMarked(e.Context) {
			wake(r.pushWake)
		}
		return nil
	}
	app.OnCollectionAfterCreateSuccess().BindFunc(wakeCollectionFn)
	app.OnCollectionAfterUpdateSuccess().BindFunc(wakeCollectionFn)
	app.OnCollectionAfterDeleteSuccess().BindFunc(wakeCollectionFn)
}

// captureRecord returns an *Execute hook func that appends an oplog row
// for a local record change, inside the write transaction.
func (r *Replicator) captureRecord(opType string) func(e *core.RecordEvent) error {
	return func(e *core.RecordEvent) error {
		if !r.ready.Load() || isMarked(e.Context) || !r.isReplicated(e.Record.Collection()) {
			return e.Next()
		}

		originalApp := e.App
		defer func() { e.App = originalApp }()
		return originalApp.RunInTransaction(func(txApp core.App) error {
			e.App = txApp
			if err := e.Next(); err != nil {
				return err
			}
			col := e.Record.Collection()
			db := txApp.NonconcurrentDB()

			seq, err := incrLocalSeq(db)
			if err != nil {
				return err
			}

			o := &op{
				SrcNode:  r.nodeID,
				SrcSeq:   seq,
				HLC:      r.clock.Now(),
				Type:     opType,
				ColID:    col.Id,
				ColName:  col.Name,
				RecordID: e.Record.Id,
			}

			if opType == opUpsert {
				payload, files, err := exportRecord(txApp, e.Record)
				if err != nil {
					return err
				}
				o.Payload = payload
				o.Files = files
			}

			if err := insertOp(db, o); err != nil {
				return err
			}
			return upsertVersion(db, o.ColID, o.RecordID, o.HLC, o.SrcNode, opType == opDelete)
		})
	}
}

// captureCollection runs after PocketBase's priority-99 Execute hook has
// opened the transaction covering collection metadata and schema DDL.
func (r *Replicator) captureCollection(opType string) func(e *core.CollectionEvent) error {
	return func(e *core.CollectionEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		if !r.ready.Load() || isMarked(e.Context) {
			return nil
		}

		db := e.App.NonconcurrentDB()

		seq, err := incrLocalSeq(db)
		if err != nil {
			return err
		}

		o := &op{
			SrcNode:  r.nodeID,
			SrcSeq:   seq,
			HLC:      r.clock.Now(),
			Type:     opType,
			ColID:    e.Collection.Id,
			ColName:  e.Collection.Name,
			RecordID: e.Collection.Id,
		}
		if opType == opColUpsert {
			raw, err := exportCollectionJSON(e.Collection)
			if err != nil {
				return err
			}
			o.Payload = raw
		}

		if err := insertOp(db, o); err != nil {
			return err
		}
		return upsertVersion(db, collectionsColID, e.Collection.Id, o.HLC, o.SrcNode, opType == opColDelete)
	}
}
