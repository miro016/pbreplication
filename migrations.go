package pbreplication

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// PocketBase's migration bookkeeping table (see core.DefaultMigrationsTable).
const migrationsTable = "_migrations"

// maybeDeferAppMigrations moves the host app's registered migrations
// out of the global core.AppMigrations on EVERY clustered start (a
// seed is configured, or peers are already known), so that apis.Serve's
// RunAllMigrations applies only system migrations. The deferred list is
// run by the bootstrap goroutine after connecting to the cluster
// (coordinateMigrations), skipping everything ANY reachable peer
// already applied - this is what prevents a data-seeding migration
// from running twice and colliding with rows arriving via sync (the
// classic duplicate-seed hazard when a migration generates random
// ids). Must run before apis.Serve, i.e. during app bootstrap.
//
// A standalone node (no seed, no known peers) never defers, so it can
// never deadlock waiting for a cluster that doesn't exist.
func (r *Replicator) maybeDeferAppMigrations(app core.App) error {
	if !*r.cfg.DeferMigrationsUntilSynced || len(core.AppMigrations.Items()) == 0 {
		return nil
	}

	clustered := r.cfg.SeedURL != ""
	if !clustered {
		members, err := listMembers(app.DB(), false)
		if err != nil {
			return err
		}
		for _, m := range members {
			if m.NodeID != r.nodeID {
				clustered = true
				break
			}
		}
	}
	if !clustered {
		return nil
	}

	r.deferredMigrations.Copy(core.AppMigrations)
	core.AppMigrations = core.MigrationsList{}
	r.migrationsDeferred = true
	r.logInfo("deferring app migrations for cluster coordination",
		"count", len(r.deferredMigrations.Items()))
	return nil
}

// listAppliedMigrations returns the migration file names recorded in
// the local _migrations table. Returns a non-nil slice on success; a
// missing table yields an empty slice.
func listAppliedMigrations(db dbx.Builder) ([]string, error) {
	files := []string{}
	err := db.NewQuery(fmt.Sprintf("SELECT file FROM {{%s}} ORDER BY file", migrationsTable)).
		Column(&files)
	if err != nil {
		var exists int
		probeErr := db.NewQuery(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = {:name}`).
			Bind(dbx.Params{"name": migrationsTable}).Row(&exists)
		if probeErr != nil || exists == 0 {
			return []string{}, nil
		}
		return nil, err
	}
	return files, nil
}

// importClusterMigrations marks the migrations the cluster already
// applied as applied locally, WITHOUT running them (their effects
// arrived with the snapshot). Only file names present in the deferred
// list are imported, so system migrations and migrations from newer
// peer binaries don't pollute the local history. seedApplied == nil
// means the seed predates this feature and cannot report its history;
// in that case ALL deferred migrations are assumed applied.
func (r *Replicator) importClusterMigrations(seedApplied []string) error {
	if !r.migrationsDeferred {
		return nil
	}

	var toMark []string
	if seedApplied == nil {
		r.logError("seed did not report its migration history (older pbreplication version?)",
			fmt.Errorf("assuming all %d deferred app migrations were already applied cluster-wide; upgrade the seed before rolling out new migrations", len(r.deferredMigrations.Items())))
		for _, m := range r.deferredMigrations.Items() {
			toMark = append(toMark, m.File)
		}
	} else {
		applied := make(map[string]bool, len(seedApplied))
		for _, f := range seedApplied {
			applied[f] = true
		}
		for _, m := range r.deferredMigrations.Items() {
			if applied[m.File] {
				toMark = append(toMark, m.File)
			}
		}
	}

	err := r.app.RunInTransaction(func(txApp core.App) error {
		db := txApp.NonconcurrentDB()
		// mirrors core.MigrationsRunner.initMigrationsTable
		if _, err := db.NewQuery(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS {{%s}} (file VARCHAR(255) PRIMARY KEY NOT NULL, applied INTEGER NOT NULL)",
			migrationsTable,
		)).Execute(); err != nil {
			return err
		}
		for _, file := range toMark {
			if _, err := db.NewQuery(fmt.Sprintf(
				"INSERT INTO {{%s}} (file, applied) VALUES ({:file}, {:applied}) ON CONFLICT(file) DO NOTHING",
				migrationsTable,
			)).Bind(dbx.Params{"file": file, "applied": time.Now().UnixMicro()}).Execute(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	r.logInfo("imported cluster migration history",
		"marked_applied", len(toMark), "deferred", len(r.deferredMigrations.Items()))
	return nil
}

// migrationsResponse is the wire shape of GET /api/replication/migrations.
type migrationsResponse struct {
	NodeID string `json:"node_id"`
	// Applied deliberately has no omitempty: nil (old peer) and []
	// (nothing applied) must stay distinguishable.
	Applied []string `json:"applied"`
}

// coordinateMigrations runs the deferred app migrations after asking
// every reachable peer which migrations it already executed: those are
// imported as applied (their effects arrived via sync/copy), and only
// migrations NO peer ran are executed locally. When no peer answers
// within MigrationCoordinationTimeout the node runs everything locally
// so it never stays schema-less. Errors are logged, not returned - the
// serve-time migration runner retries unapplied files on the next
// start anyway.
func (r *Replicator) coordinateMigrations() {
	if !r.migrationsDeferred {
		return
	}

	union, reachable := r.collectPeerMigrations()
	if reachable > 0 {
		if err := r.importClusterMigrations(union); err != nil {
			r.logError("importing coordinated migration history", err)
		}
		// Narrow the simultaneous-restart race: when several nodes hold
		// the same brand-new migration, non-leaders give the leader one
		// sync interval to run + gossip it, then look again. Not a
		// distributed lock - migrations that seed data should still be
		// written idempotently.
		if !r.IsLeader() && r.unappliedDeferredCount() > 0 {
			select {
			case <-r.stopCh:
				return
			case <-time.After(r.cfg.SyncInterval):
			}
			if union, reachable = r.collectPeerMigrations(); reachable > 0 {
				if err := r.importClusterMigrations(union); err != nil {
					r.logError("importing coordinated migration history (recheck)", err)
				}
			}
		}
	} else {
		r.logWarn("no peer answered the migration coordination query - running deferred migrations locally",
			"deferred", len(r.deferredMigrations.Items()))
	}

	if err := r.runDeferredMigrations(); err != nil {
		r.logError("running deferred app migrations", err)
	}
}

// collectPeerMigrations queries all members with a URL in parallel and
// returns the union of their applied-migration lists plus how many
// peers answered.
func (r *Replicator) collectPeerMigrations() ([]string, int) {
	members, err := listMembers(r.app.DB(), false)
	if err != nil {
		return []string{}, 0
	}

	type result struct {
		applied []string
		ok      bool
	}
	ctx, cancel := context.WithTimeout(r.runCtx, r.cfg.MigrationCoordinationTimeout)
	defer cancel()

	results := make(chan result, len(members))
	queried := 0
	for _, m := range members {
		if m.NodeID == r.nodeID || m.URL == "" {
			continue
		}
		queried++
		go func(m *member) {
			var resp migrationsResponse
			err := r.callPeerCtx(ctx, r.peerURL(m), http.MethodGet, "/api/replication/migrations", nil, &resp)
			results <- result{applied: resp.Applied, ok: err == nil && resp.Applied != nil}
		}(m)
	}

	seen := map[string]bool{}
	union := []string{}
	reachable := 0
	for i := 0; i < queried; i++ {
		res := <-results
		if !res.ok {
			continue
		}
		reachable++
		for _, f := range res.applied {
			if !seen[f] {
				seen[f] = true
				union = append(union, f)
			}
		}
	}
	return union, reachable
}

// unappliedDeferredCount reports how many deferred migrations are not
// yet recorded in the local _migrations table.
func (r *Replicator) unappliedDeferredCount() int {
	applied, err := listAppliedMigrations(r.app.DB())
	if err != nil {
		return len(r.deferredMigrations.Items())
	}
	have := map[string]bool{}
	for _, f := range applied {
		have[f] = true
	}
	n := 0
	for _, item := range r.deferredMigrations.Items() {
		if !have[item.File] {
			n++
		}
	}
	return n
}

// runDeferredMigrations executes the deferred app migrations that the
// cluster has NOT applied yet (the runner skips files already recorded
// in _migrations). Their writes go through the normal PocketBase save
// pipeline, so the capture hooks replicate them to the cluster.
func (r *Replicator) runDeferredMigrations() error {
	if !r.migrationsDeferred {
		return nil
	}

	pending := len(r.deferredMigrations.Items())
	r.logMilestone("running post-sync app migrations", "pending", pending)

	applied, err := core.NewMigrationsRunner(r.app, r.deferredMigrations).Up()
	if err != nil {
		return err
	}
	for _, file := range applied {
		r.logInfo("applied post-sync migration", "file", file)
		r.console("  applied migration %q", file)
	}

	r.logMilestone("post-sync app migrations complete", "applied", len(applied), "skipped", pending-len(applied))
	r.emitEvent(EventMigrationRun, "post-sync app migrations complete",
		"applied", len(applied), "skipped", pending-len(applied))

	r.migrationsDeferred = false
	r.deferredMigrations = core.MigrationsList{}
	return nil
}
