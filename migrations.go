package pbreplication

import (
	"fmt"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// PocketBase's migration bookkeeping table (see core.DefaultMigrationsTable).
const migrationsTable = "_migrations"

// maybeDeferAppMigrations moves the host app's registered migrations
// out of the global core.AppMigrations on a fresh node that will
// bootstrap from a seed, so that apis.Serve's RunAllMigrations applies
// only system migrations. The deferred list is run by the bootstrap
// goroutine after the initial snapshot sync (runDeferredMigrations),
// skipping everything the cluster already applied. Must run before
// apis.Serve, i.e. during app bootstrap.
func (r *Replicator) maybeDeferAppMigrations(app core.App) error {
	if !*r.cfg.DeferMigrationsUntilSynced || r.cfg.SeedURL == "" {
		return nil
	}
	done, err := getState(app.DB(), stateBootstrapDone)
	if err != nil {
		return err
	}
	if done != "" || len(core.AppMigrations.Items()) == 0 {
		return nil
	}

	r.deferredMigrations.Copy(core.AppMigrations)
	core.AppMigrations = core.MigrationsList{}
	r.migrationsDeferred = true
	r.logInfo("deferring app migrations until the initial sync completes",
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

	r.migrationsDeferred = false
	r.deferredMigrations = core.MigrationsList{}
	return nil
}
