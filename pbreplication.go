// Package pbreplication extends PocketBase with active-active
// (multi-master) replication between instances.
//
// Every node accepts writes; conflicts are resolved with
// last-update-wins semantics based on hybrid logical clocks. All
// replication traffic rides on PocketBase's regular HTTP port under
// /api/replication/*, authenticated with a shared cluster secret, so no
// extra ports are needed. Nodes discover each other automatically: a
// new node only needs the URL of one existing member (the "seed") and
// the cluster secret.
//
// Usage:
//
//	app := pocketbase.New()
//	pbreplication.MustRegister(app, pbreplication.Config{
//		NodeURL:       "http://node1:8090",
//		SeedURL:       "http://node2:8090", // empty for the very first node
//		ClusterSecret: "at-least-16-characters-long",
//	})
//	app.Start()
package pbreplication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/security"
)

// newPeerTransport builds the shared HTTP transport for node-to-node
// calls: bounded dial and response-header phases, but no overall
// request timeout (per-call deadlines come from contexts).
func newPeerTransport() *http.Transport {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
}

// Config configures a cluster node.
type Config struct {
	// NodeURL is the base URL under which OTHER nodes can reach this
	// node (e.g. "https://pb1.example.com"). Behind a reverse proxy set
	// it to the public URL. Leave empty to run in "pull-only" mode:
	// the node still replicates fully but always initiates the
	// exchanges itself (useful behind NAT).
	NodeURL string

	// SeedURL is the URL of any existing cluster member. Leave empty
	// only for the very first node of a new cluster.
	SeedURL string

	// ClusterSecret is the shared cluster password (min 16 chars).
	ClusterSecret string

	// NodeID optionally fixes the node id. When empty an id is
	// generated on first start and persisted in the database.
	NodeID string

	// SyncInterval is the anti-entropy pull period. Default: 10s.
	SyncInterval time.Duration

	// DebounceWindow batches rapid local writes before pushing.
	// Default: 150ms.
	DebounceWindow time.Duration

	// MaxBatch limits ops per push/pull page. Default: 500.
	MaxBatch int

	// TombstoneRetention is how long delete markers are kept. It must
	// exceed the longest planned node downtime; nodes offline for
	// longer automatically fall back to a full snapshot resync.
	// Default: 720h (30 days).
	TombstoneRetention time.Duration

	// CompactionInterval is how often the oplog is compacted.
	// Default: 1h.
	CompactionInterval time.Duration

	// ExcludeCollections lists collections that must NOT replicate.
	// Default: _mfas, _otps, _authOrigins (node-local auth artifacts).
	ExcludeCollections []string

	// ReplicateSuperusers controls replication of the _superusers
	// collection. Default: true.
	ReplicateSuperusers *bool

	// DeferMigrationsUntilSynced makes a fresh node that joins an
	// existing cluster (SeedURL set, first start) postpone the host
	// app's migrations until AFTER the initial full snapshot sync, and
	// then run only those the cluster hasn't already applied. This
	// avoids re-running migrations and seeds whose effects already
	// exist in the cluster. Default: true.
	DeferMigrationsUntilSynced *bool

	// DisableUIExtension turns off the "Replication" tab that is
	// injected into the PocketBase admin UI via PocketBase's
	// experimental UI extension API. The standalone dashboard at
	// /api/replication/dashboard always works. Default: false (enabled).
	DisableUIExtension bool

	// GeoIPDBPath optionally points to a MaxMind-format .mmdb database
	// used to resolve country/region firewall rules and to geolocate
	// client IPs for the dashboard map. When empty, the embedded DB-IP
	// Country Lite database is used, so country rules work out of the
	// box. Point this at a city-level database (e.g. GeoLite2-City or
	// DB-IP City Lite) to also enable region rules and city/coordinate
	// resolution on the map.
	GeoIPDBPath string

	// DisableEmbeddedGeoIP skips loading the embedded DB-IP Country
	// Lite database (~8 MB of memory). Without it and without a
	// GeoIPDBPath, country/region firewall rules are ignored (a warning
	// is logged and shown in the dashboard) and client IPs are not
	// geolocated locally. Default: false.
	DisableEmbeddedGeoIP bool

	// FirewallExemptSuperusers lets requests with a valid superuser
	// token bypass app-scope firewall rules so an admin can't lock
	// themself out. Default: true.
	FirewallExemptSuperusers *bool

	// DisableIPGeolocation turns off the automatic geolocation of new
	// client IPs entirely (used by the dashboard map). Client IPs are
	// still counted; they just won't be located. Default: false.
	DisableIPGeolocation bool

	// EnableIPAPIGeolocation switches client-IP geolocation to the
	// external ip-api.com service, which adds city and coordinates
	// (better map dots) at the cost of sending client IPs to a third
	// party. Default: false — clients are resolved locally from the
	// GeoIP database (embedded or GeoIPDBPath) and no external
	// geolocation calls are ever made.
	EnableIPAPIGeolocation bool

	// IPAPIKey is an optional ip-api.com paid ("pro") API key. Setting
	// it implies EnableIPAPIGeolocation and uses the HTTPS pro endpoint
	// (higher rate limit, no public-network throttling).
	IPAPIKey string

	// RequestTimeout bounds a single node-to-node JSON request
	// (push/pull/join/meta pages). Streaming transfers (blobs, database
	// snapshot chunks) are NOT subject to it; they use per-chunk
	// deadlines instead so large transfers survive slow links.
	// Default: 30s.
	RequestTimeout time.Duration

	// MaxBodyBytes caps how much of a node-to-node request body is
	// buffered in memory for HMAC verification. Push/pull payloads are
	// already bounded by MaxBatch, so the default is generous.
	// Default: 16MB.
	MaxBodyBytes int64

	// EventBufferSize is the capacity of the in-memory replication
	// event ring buffer served at /api/replication/events and via
	// (*Replicator).Events. Default: 512.
	EventBufferSize int

	// FullCopyBootstrap makes a NEW node (no local database yet) copy
	// the seed's whole SQLite database file instead of syncing row by
	// row - orders of magnitude faster for large databases. The copy
	// happens BEFORE PocketBase opens the database, so serve-time
	// migrations run only the files the cluster hasn't applied and the
	// node starts already in sync. Old seeds without snapshot support
	// fall back to the logical sync automatically. Default: true.
	FullCopyBootstrap *bool

	// FullCopyChunkSize is the transfer chunk for database snapshot
	// downloads. Each chunk is fetched (and retried) independently, so
	// unstable links resume instead of restarting. Default: 8MB.
	FullCopyChunkSize int

	// FullCopyFallbackAfter bounds how long a failing full copy is
	// retried before the node falls back to the logical bootstrap.
	// Default: 10m.
	FullCopyFallbackAfter time.Duration

	// SnapshotCacheTTL is how long a prepared database snapshot file is
	// reused for additional joiners before a fresh one is vacuumed.
	// Default: 10m.
	SnapshotCacheTTL time.Duration

	// IntegrityCheckAfterSync runs a relation-integrity validation pass
	// (dangling reference scan) after bulk syncs complete, re-checking
	// while the node converges. Results surface in the logs, the event
	// timeline, /status and LastIntegrityReport(). Default: true.
	IntegrityCheckAfterSync *bool

	// MigrationCoordinationTimeout bounds how long a starting node waits
	// for peers to report their executed migrations before falling back
	// to running the deferred migrations locally. Default: 30s.
	MigrationCoordinationTimeout time.Duration

	// ResyncStrategy selects how a node that fell behind compaction
	// (snapshot_required) catches up:
	//   "logical"      - row-by-row snapshot sync in-process (default)
	//   "restart-copy" - flag the node (resync_pending) and ask for a
	//                    restart; the next start replaces the database
	//                    with a full copy, rescuing un-synced local
	//                    writes first. Best for very large databases.
	ResyncStrategy string
}

func (c *Config) setDefaults() {
	if c.SyncInterval <= 0 {
		c.SyncInterval = 10 * time.Second
	}
	if c.DebounceWindow <= 0 {
		c.DebounceWindow = 150 * time.Millisecond
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 500
	}
	if c.TombstoneRetention <= 0 {
		c.TombstoneRetention = 720 * time.Hour
	}
	if c.CompactionInterval <= 0 {
		c.CompactionInterval = time.Hour
	}
	if c.ExcludeCollections == nil {
		c.ExcludeCollections = []string{"_mfas", "_otps", "_authOrigins"}
	}
	if c.ReplicateSuperusers == nil {
		v := true
		c.ReplicateSuperusers = &v
	}
	if c.DeferMigrationsUntilSynced == nil {
		v := true
		c.DeferMigrationsUntilSynced = &v
	}
	if c.FirewallExemptSuperusers == nil {
		v := true
		c.FirewallExemptSuperusers = &v
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 16 << 20
	}
	if c.EventBufferSize <= 0 {
		c.EventBufferSize = 512
	}
	if c.FullCopyBootstrap == nil {
		v := true
		c.FullCopyBootstrap = &v
	}
	if c.FullCopyChunkSize <= 0 {
		c.FullCopyChunkSize = 8 << 20
	}
	if c.FullCopyFallbackAfter <= 0 {
		c.FullCopyFallbackAfter = 10 * time.Minute
	}
	if c.SnapshotCacheTTL <= 0 {
		c.SnapshotCacheTTL = 10 * time.Minute
	}
	if c.IntegrityCheckAfterSync == nil {
		v := true
		c.IntegrityCheckAfterSync = &v
	}
	if c.MigrationCoordinationTimeout <= 0 {
		c.MigrationCoordinationTimeout = 30 * time.Second
	}
	if c.ResyncStrategy == "" {
		c.ResyncStrategy = "logical"
	}
	c.NodeURL = strings.TrimRight(c.NodeURL, "/")
	c.SeedURL = strings.TrimRight(c.SeedURL, "/")
}

func (c *Config) validate() error {
	if len(c.ClusterSecret) < 16 {
		return errors.New("pbreplication: ClusterSecret must be at least 16 characters")
	}
	if c.ResyncStrategy != "logical" && c.ResyncStrategy != "restart-copy" {
		return fmt.Errorf("pbreplication: invalid ResyncStrategy %q (want \"logical\" or \"restart-copy\")", c.ResyncStrategy)
	}
	return nil
}

// Replicator is the running replication engine of a node.
type Replicator struct {
	app core.App
	cfg Config

	clock  *hlc
	nodeID string
	ready  atomic.Bool

	// instanceID identifies this PROCESS and is never persisted. Two
	// processes sharing a nodeID but differing here are the signature of
	// a cloned data directory (see identity.go).
	instanceID string

	// jsonClient serves bounded request/response exchanges; each call
	// carries its own context deadline (cfg.RequestTimeout). streamClient
	// has no overall timeout so long-running streams (blobs, database
	// snapshot chunks) are not killed mid-transfer; it still bounds the
	// dial and response-header phases.
	jsonClient   *http.Client
	streamClient *http.Client

	// runCtx is cancelled on shutdown; all outgoing peer requests are
	// derived from it.
	runCtx    context.Context
	runCancel context.CancelFunc

	pushWake chan struct{}
	pullWake chan struct{}
	applyCh  chan *op
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// per-peer oplog rowid push cursors (in-memory; anti-entropy heals)
	cursorMu    sync.Mutex
	pushCursors map[string]int64
	startRowID  int64 // oplog head at process start

	// ops waiting for a not-yet-known collection
	pendingMu  sync.Mutex
	pendingOps []*op

	// per-peer URL overrides: when a member's advertised URL is not
	// reachable from THIS node (e.g. a docker-internal name gossiped to
	// a node on another host) but another URL demonstrably works (the
	// configured seed URL), connections use the override instead.
	urlOverrides sync.Map // nodeID -> url

	// last sync error per peer (empty entry = healthy), for the dashboard
	memberErrs sync.Map // nodeID -> string

	// replication event timeline (ring buffer + subscribers)
	events *eventLog

	// live bulk-sync progress (snapshot / full copy / integrity check)
	progressState atomic.Pointer[SyncStatus]

	// last observed health per peer, for transition detection
	healthMu   sync.Mutex
	prevHealth map[string]bool

	// most recent vector reported by each peer (from push/pull
	// responses); the basis for replication-lag reporting
	peerVectors sync.Map // nodeID -> map[string]int64

	// last time the periodic lag summary was logged
	lastLagLog atomic.Int64 // unix seconds

	// throttle for op-failure events (per collection)
	opFailMu   sync.Mutex
	opFailLast map[string]time.Time

	// buffered per-client-IP request counters (flushed in batches)
	clientCounts sync.Map // ip -> *clientCounter
	// buffered per-(ip,method,path) counters
	pathCounts sync.Map // "ip\x00method\x00path" -> *pathCounter
	// geolocation resolver, overridable in tests
	geoLookup func(ip string) (*geoResult, error)

	// blobs that could not be fetched from any peer yet
	blobMu          sync.Mutex
	missingBlobList []*missingBlob

	// excluded collection lookup
	excluded map[string]bool

	// guards concurrent snapshot resyncs
	resyncInFlight atomic.Bool

	// prepared database snapshot cache (server side of the full copy)
	dbSnapMu       sync.Mutex
	dbSnapManifest *dbSnapshotManifest
	dbSnapCreated  time.Time

	// guards concurrent blob backfill passes
	blobBackfillInFlight atomic.Bool

	// relation-integrity validation state
	integrityPending  atomic.Bool
	integrityInFlight atomic.Bool
	lastIntegrity     atomic.Pointer[IntegrityReport]

	// app migrations held back until after the initial snapshot sync.
	// Written once in initStorage (before apis.Serve/startBackground),
	// afterwards read only by the bootstrap goroutine.
	deferredMigrations core.MigrationsList
	migrationsDeferred bool

	firewall *firewall

	stats struct {
		applied   atomic.Int64
		failed    atomic.Int64
		blocked   atomic.Int64
		lastError atomic.Value // string
	}
}

const maxPendingOps = 10000

// Register wires the replication engine into the given PocketBase app.
// Call it before app.Start().
func Register(app core.App, cfg Config) (*Replicator, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	r := &Replicator{
		app:        app,
		cfg:        cfg,
		clock:      newHLC(),
		instanceID: security.RandomString(15),
		jsonClient: &http.Client{
			// no flat Timeout: every call sets a context deadline
			Transport: newPeerTransport(),
		},
		streamClient: &http.Client{
			// no overall timeout: streams are bounded per chunk by the
			// caller; the transport still bounds dial + header phases
			Transport: newPeerTransport(),
		},
		runCtx:      runCtx,
		runCancel:   runCancel,
		pushWake:    make(chan struct{}, 1),
		pullWake:    make(chan struct{}, 1),
		applyCh:     make(chan *op, 4096),
		stopCh:      make(chan struct{}),
		pushCursors: map[string]int64{},
		excluded:    map[string]bool{},
		events:      newEventLog(cfg.EventBufferSize),
		prevHealth:  map[string]bool{},
		opFailLast:  map[string]time.Time{},
	}
	for _, name := range cfg.ExcludeCollections {
		r.excluded[name] = true
	}
	if !*cfg.ReplicateSuperusers {
		r.excluded[core.CollectionNameSuperusers] = true
	}
	r.firewall = newFirewall(r)

	app.OnBootstrap().BindFunc(func(e *core.BootstrapEvent) error {
		// BEFORE PocketBase opens its database: a new node (or one
		// flagged for resync) installs a full copy of the cluster's
		// database first, so PB boots directly on synced data and
		// serve-time migrations run only what the cluster hasn't.
		if err := r.maybeFullCopyBootstrap(e.App); err != nil {
			return err
		}
		if err := e.Next(); err != nil {
			return err
		}
		return r.initStorage(e.App)
	})

	r.bindCaptureHooks(app)
	r.bindFirewallHooks(app)

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		r.registerRoutes(se)
		r.firewall.bindMiddleware(se)
		if !cfg.DisableUIExtension {
			r.registerUIExtension(se)
		}
		r.startBackground()
		return se.Next()
	})

	app.OnTerminate().BindFunc(func(e *core.TerminateEvent) error {
		r.shutdown()
		return e.Next()
	})

	return r, nil
}

// MustRegister is like Register but panics on configuration errors.
func MustRegister(app core.App, cfg Config) *Replicator {
	r, err := Register(app, cfg)
	if err != nil {
		panic(err)
	}
	return r
}

// NodeID returns this node's persistent id (available after bootstrap).
func (r *Replicator) NodeID() string {
	return r.nodeID
}

// initStorage prepares tables, node identity and the firewall
// collection. Runs once on app bootstrap.
func (r *Replicator) initStorage(app core.App) error {
	if err := createTables(app); err != nil {
		return err
	}

	db := app.NonconcurrentDB()

	// node identity
	id, err := getState(db, stateNodeID)
	if err != nil {
		return err
	}
	if id == "" {
		id = r.cfg.NodeID
		if id == "" {
			id = security.RandomString(15)
		}
		if err := setState(db, stateNodeID, id); err != nil {
			return err
		}
	}
	r.nodeID = id

	// resume the clock from the last persisted timestamp
	if persisted, _ := getState(db, stateHLC); persisted != "" {
		r.clock.Resume(persisted)
	}

	// Ensure the self membership row exists, but DON'T overwrite a
	// previously stored URL here: initStorage also runs for one-off CLI
	// commands (e.g. "superuser upsert") that may carry a different or
	// empty NodeURL and must not clobber the advertised URL. The URL is
	// (re)set authoritatively only when actually serving, in
	// startBackground.
	if existing, _ := getMember(db, r.nodeID); existing == nil {
		if err := upsertMember(db, &member{
			NodeID:    r.nodeID,
			URL:       r.cfg.NodeURL,
			Reachable: r.cfg.NodeURL != "",
			LastSeen:  nowStr(),
		}); err != nil {
			return err
		}
	}

	if err := r.ensureFirewallCollection(app); err != nil {
		return err
	}
	r.firewall.reload(app)

	if err := r.maybeDeferAppMigrations(app); err != nil {
		return err
	}

	r.ready.Store(true)
	return nil
}

func (r *Replicator) startBackground() {
	// The initial push cursor must predate any op re-emitted by the
	// duplicate-id resolution below, so those ops still reach the peers.
	head, headErr := maxRowID(r.app.DB())

	// Still pre-serve: nothing reads r.nodeID concurrently yet, so a
	// duplicated identity (cloned data directory) can be swapped safely.
	r.resolveDuplicateNodeID()

	// Now that we are actually serving as a node, set our advertised
	// URL authoritatively (a restart may use a new NodeURL).
	_ = upsertMember(r.app.NonconcurrentDB(), &member{
		NodeID:    r.nodeID,
		URL:       r.cfg.NodeURL,
		Reachable: r.cfg.NodeURL != "",
		LastSeen:  nowStr(),
	})

	if headErr == nil {
		r.cursorMu.Lock()
		r.startRowID = head
		r.cursorMu.Unlock()
	}

	r.wg.Add(5)
	go r.pushLoop()
	go r.antiEntropyLoop()
	go r.applyLoop()
	go r.compactLoop()
	go r.geoLoop()

	go func() {
		// Keep retrying until the initial bootstrap succeeds: a fresh
		// node with deferred migrations has no app schema at all until
		// the first snapshot lands.
		for {
			err := r.bootstrapOrRejoin()
			if err == nil {
				return
			}
			r.logError("bootstrap failed (retrying)", err)
			select {
			case <-r.stopCh:
				return
			case <-time.After(r.cfg.SyncInterval):
			}
		}
	}()
}

func (r *Replicator) shutdown() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
		r.runCancel()
	})
	r.wg.Wait()
	if r.nodeID != "" {
		_ = setState(r.app.NonconcurrentDB(), stateHLC, r.clock.Current())
	}
	if r.firewall != nil {
		r.firewall.close()
	}
	// prepared snapshot files are cheap to regenerate - don't keep them
	if r.app != nil && r.app.DataDir() != "" {
		_ = os.RemoveAll(filepath.Join(r.app.DataDir(), dbSnapshotDir))
	}
}

// isReplicated reports whether changes in the given collection should
// be captured and replicated.
func (r *Replicator) isReplicated(col *core.Collection) bool {
	if col == nil || col.IsView() {
		return false
	}
	return !r.excluded[col.Name] && !r.excluded[col.Id]
}

// log routes a message to the PocketBase logger with the standard
// component attribute prepended, so replication entries are filterable
// in the _logs table.
func (r *Replicator) log(level slog.Level, msg string, args ...any) {
	if r.app == nil || r.app.Logger() == nil {
		return
	}
	all := make([]any, 0, len(args)+2)
	all = append(all, slog.String("component", "pbreplication"))
	all = append(all, args...)
	r.app.Logger().Log(context.Background(), level, "pbreplication: "+msg, all...)
}

func (r *Replicator) logError(msg string, err error, args ...any) {
	r.stats.lastError.Store(fmt.Sprintf("%s: %v", msg, err))
	all := append([]any{slog.String("error", err.Error())}, args...)
	r.log(slog.LevelError, msg, all...)
}

func (r *Replicator) logWarn(msg string, args ...any) {
	r.log(slog.LevelWarn, msg, args...)
}

func (r *Replicator) logInfo(msg string, args ...any) {
	r.log(slog.LevelInfo, msg, args...)
}

// logMilestone records a notable lifecycle event (instance connected,
// migration started/finished, ...) to BOTH the PocketBase logger (so it
// is persisted in the _logs table and visible in the admin UI) and the
// process stdout (so operators watching the console see it live, even
// though the PocketBase logger does not print there).
func (r *Replicator) logMilestone(msg string, args ...any) {
	r.logInfo(msg, args...)
	fmt.Fprintln(os.Stdout, formatConsoleLine(msg, args...))
}

// console prints a formatted line to stdout with the standard
// pbreplication prefix. Used for informational output that complements
// the persisted logs (e.g. per-collection sync summaries).
func (r *Replicator) console(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "%s [pbreplication] %s\n",
		time.Now().Format("2006/01/02 15:04:05"), fmt.Sprintf(format, args...))
}

// consoleProgress updates a single, in-place console line (carriage
// return, no newline) so a long-running sync can show live progress
// without flooding the terminal. Call consoleProgressDone to terminate
// the line once the operation completes.
func (r *Replicator) consoleProgress(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "\r%s [pbreplication] %s",
		time.Now().Format("2006/01/02 15:04:05"), fmt.Sprintf(format, args...))
}

// consoleProgressDone finalizes an in-place progress line with a final
// message and a trailing newline.
func (r *Replicator) consoleProgressDone(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "\r\033[K%s [pbreplication] %s\n",
		time.Now().Format("2006/01/02 15:04:05"), fmt.Sprintf(format, args...))
}

// formatConsoleLine renders a message plus slog-style key/value args
// into a single human-readable console line.
func formatConsoleLine(msg string, args ...any) string {
	var b strings.Builder
	b.WriteString(time.Now().Format("2006/01/02 15:04:05"))
	b.WriteString(" [pbreplication] ")
	b.WriteString(msg)
	for i := 0; i+1 < len(args); i += 2 {
		fmt.Fprintf(&b, " %v=%v", args[i], args[i+1])
	}
	if len(args)%2 == 1 {
		fmt.Fprintf(&b, " %v", args[len(args)-1])
	}
	return b.String()
}

// wake performs a non-blocking signal on a wake channel.
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
