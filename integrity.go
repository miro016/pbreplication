package pbreplication

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// Relation-integrity validation. Applying replicated rows can never
// fail on a missing relation target (relations are plain text values
// and applies bypass validation by design - eventual consistency fills
// the gaps as ops arrive). What CAN linger is a dangling reference:
// a relation pointing at a record that never arrives (e.g. deleted on
// another node in the same window). This validator scans all relation
// fields after bulk syncs, re-checking a few rounds while the node is
// still converging, and surfaces whatever remains via logs, the event
// timeline, /status and the exported API.

// DanglingRef is one relation value pointing at a nonexistent record.
type DanglingRef struct {
	Collection       string `json:"collection"`
	RecordID         string `json:"record_id"`
	Field            string `json:"field"`
	TargetCollection string `json:"target_collection"`
	TargetID         string `json:"target_id"`
}

// IntegrityReport is the result of one relation-integrity check.
type IntegrityReport struct {
	StartedAt      time.Time     `json:"started_at"`
	FinishedAt     time.Time     `json:"finished_at"`
	ScannedRecords int64         `json:"scanned_records"`
	CheckedRefs    int64         `json:"checked_refs"`
	Dangling       []DanglingRef `json:"dangling"`
	// TruncatedAt is the total number of dangling refs found when the
	// report list was capped (0 = complete list).
	TruncatedAt int `json:"truncated_at,omitempty"`
	// Converged is true when the final re-check found no dangling refs.
	Converged bool `json:"converged"`
}

// maxReportedDangling caps the report size; the total count is still
// tracked in TruncatedAt.
const maxReportedDangling = 500

// RunIntegrityCheck scans every replicated collection's relation fields
// and verifies that each referenced record exists. Records are paged
// and targets are verified in chunked IN() queries, so memory stays
// bounded regardless of database size.
func (r *Replicator) RunIntegrityCheck(ctx context.Context) (*IntegrityReport, error) {
	report := &IntegrityReport{StartedAt: time.Now()}

	cols, err := r.app.FindAllCollections()
	if err != nil {
		return nil, err
	}

	total := 0
	for _, col := range cols {
		if !r.isReplicated(col) {
			continue
		}
		var relFields []*core.RelationField
		for _, f := range col.Fields {
			if rf, ok := f.(*core.RelationField); ok {
				relFields = append(relFields, rf)
			}
		}
		if len(relFields) == 0 {
			continue
		}
		if err := r.checkCollectionIntegrity(ctx, col, relFields, report, &total); err != nil {
			return nil, err
		}
	}

	report.FinishedAt = time.Now()
	report.Converged = total == 0
	if total > len(report.Dangling) {
		report.TruncatedAt = total
	}
	return report, nil
}

func (r *Replicator) checkCollectionIntegrity(ctx context.Context, col *core.Collection, relFields []*core.RelationField, report *IntegrityReport, total *int) error {
	const pageSize = 500
	after := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return context.Canceled
		default:
		}

		records := []*core.Record{}
		err := r.app.RecordQuery(col).
			AndWhere(dbx.NewExp("id > {:after}", dbx.Params{"after": after})).
			OrderBy("id ASC").Limit(pageSize).All(&records)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		after = records[len(records)-1].Id
		report.ScannedRecords += int64(len(records))

		for _, rf := range relFields {
			target, _ := r.app.FindCachedCollectionByNameOrId(rf.CollectionId)

			// referenced ids of this page, deduplicated
			refs := map[string][]string{} // targetID -> referencing record ids
			for _, rec := range records {
				for _, targetID := range rec.GetStringSlice(rf.Name) {
					if targetID == "" {
						continue
					}
					refs[targetID] = append(refs[targetID], rec.Id)
				}
			}
			if len(refs) == 0 {
				continue
			}
			report.CheckedRefs += int64(len(refs))

			missing := []string{}
			if target == nil {
				// the whole target collection is gone - every ref dangles
				for id := range refs {
					missing = append(missing, id)
				}
			} else {
				ids := make([]string, 0, len(refs))
				for id := range refs {
					ids = append(ids, id)
				}
				found, err := existingIDs(r.app.DB(), target.Name, ids)
				if err != nil {
					return err
				}
				for _, id := range ids {
					if !found[id] {
						missing = append(missing, id)
					}
				}
			}

			targetName := rf.CollectionId
			if target != nil {
				targetName = target.Name
			}
			for _, targetID := range missing {
				for _, recID := range refs[targetID] {
					*total++
					if len(report.Dangling) < maxReportedDangling {
						report.Dangling = append(report.Dangling, DanglingRef{
							Collection:       col.Name,
							RecordID:         recID,
							Field:            rf.Name,
							TargetCollection: targetName,
							TargetID:         targetID,
						})
					}
				}
			}
		}

		if len(records) < pageSize {
			return nil
		}
	}
}

// existingIDs reports which of the given ids exist in the table, using
// chunked IN() queries.
func existingIDs(db dbx.Builder, table string, ids []string) (map[string]bool, error) {
	found := map[string]bool{}
	const chunk = 200
	for len(ids) > 0 {
		n := len(ids)
		if n > chunk {
			n = chunk
		}
		params := dbx.Params{}
		placeholders := make([]string, n)
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("i%d", i)
			placeholders[i] = "{:" + key + "}"
			params[key] = ids[i]
		}
		var present []string
		err := db.NewQuery(fmt.Sprintf(`SELECT id FROM {{%s}} WHERE id IN (%s)`,
			table, strings.Join(placeholders, ","))).Bind(params).Column(&present)
		if err != nil {
			return nil, err
		}
		for _, id := range present {
			found[id] = true
		}
		ids = ids[n:]
	}
	return found, nil
}

// LastIntegrityReport returns the most recent relation-integrity report
// (nil when none ran yet).
func (r *Replicator) LastIntegrityReport() *IntegrityReport {
	return r.lastIntegrity.Load()
}

// scheduleIntegrityCheck flags that a bulk sync completed and an
// integrity pass should run once the node quiesces.
func (r *Replicator) scheduleIntegrityCheck() {
	if *r.cfg.IntegrityCheckAfterSync {
		r.integrityPending.Store(true)
	}
}

// maybeRunIntegrity runs the scheduled integrity check when the node
// has quiesced (no queued/parked ops, no bulk sync in flight). Called
// from sync rounds. While dangling refs remain it re-checks up to 3
// rounds spaced one SyncInterval apart - they usually converge as the
// remaining ops arrive.
func (r *Replicator) maybeRunIntegrity() {
	if !r.integrityPending.Load() {
		return
	}
	if r.pendingCount() > 0 || len(r.applyCh) > 0 || r.resyncInFlight.Load() {
		return // still converging; try again next round
	}
	if !r.integrityInFlight.CompareAndSwap(false, true) {
		return
	}
	r.integrityPending.Store(false)

	go func() {
		defer r.integrityInFlight.Store(false)

		published := false
		if r.SyncStatus().Phase == SyncIdle {
			r.publishProgress(SyncStatus{Phase: SyncIntegrityCheck, StartedAt: time.Now()})
			published = true
		}
		if published {
			defer r.clearProgress()
		}

		var report *IntegrityReport
		for attempt := 1; ; attempt++ {
			var err error
			report, err = r.RunIntegrityCheck(r.runCtx)
			if err != nil {
				r.logError("relation integrity check failed", err)
				return
			}
			if report.Converged || attempt >= 3 {
				break
			}
			r.logInfo("dangling relations found - waiting for convergence before re-check",
				"dangling", danglingTotal(report), "attempt", attempt)
			select {
			case <-r.stopCh:
				return
			case <-time.After(r.cfg.SyncInterval):
			}
		}

		r.lastIntegrity.Store(report)
		r.reportIntegrity(report)
	}()
}

func danglingTotal(rep *IntegrityReport) int {
	if rep.TruncatedAt > 0 {
		return rep.TruncatedAt
	}
	return len(rep.Dangling)
}

func (r *Replicator) reportIntegrity(rep *IntegrityReport) {
	total := danglingTotal(rep)
	if rep.Converged {
		r.logMilestone("relation integrity check passed",
			"records", rep.ScannedRecords, "refs", rep.CheckedRefs)
	} else {
		r.logWarn("relation integrity check found dangling references",
			"records", rep.ScannedRecords, "refs", rep.CheckedRefs, "dangling", total)
	}
	r.emitEvent(EventIntegrityReport, "relation integrity check finished",
		"converged", rep.Converged, "dangling", total,
		"records", rep.ScannedRecords)
}

// handleIntegrity serves the last integrity report.
func (r *Replicator) handleIntegrity(e *core.RequestEvent) error {
	rep := r.LastIntegrityReport()
	if rep == nil {
		return e.JSON(http.StatusOK, map[string]any{"report": nil})
	}
	return e.JSON(http.StatusOK, map[string]any{"report": rep})
}

// handleIntegrityRun runs a fresh integrity check synchronously and
// returns its report.
func (r *Replicator) handleIntegrityRun(e *core.RequestEvent) error {
	if !r.integrityInFlight.CompareAndSwap(false, true) {
		return e.BadRequestError("an integrity check is already running", nil)
	}
	defer r.integrityInFlight.Store(false)

	rep, err := r.RunIntegrityCheck(e.Request.Context())
	if err != nil {
		return e.InternalServerError("integrity check failed", nil)
	}
	r.lastIntegrity.Store(rep)
	r.reportIntegrity(rep)
	return e.JSON(http.StatusOK, map[string]any{"report": rep})
}
