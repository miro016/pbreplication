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
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/security"
)

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

	// DisableUIExtension turns off the "Replication" tab that is
	// injected into the PocketBase admin UI via PocketBase's
	// experimental UI extension API. The standalone dashboard at
	// /api/replication/dashboard always works. Default: false (enabled).
	DisableUIExtension bool

	// GeoIPDBPath optionally points to a MaxMind-format .mmdb database
	// used to resolve country/region firewall rules.
	GeoIPDBPath string

	// FirewallExemptSuperusers lets requests with a valid superuser
	// token bypass app-scope firewall rules so an admin can't lock
	// themself out. Default: true.
	FirewallExemptSuperusers *bool

	// DisableIPGeolocation turns off the automatic geolocation of new
	// client IPs via ip-api.com (used by the dashboard map). Client IPs
	// are still counted; they just won't be located. Default: false.
	DisableIPGeolocation bool

	// IPAPIKey is an optional ip-api.com paid ("pro") API key. When set,
	// geolocation uses the HTTPS pro endpoint (higher rate limit, no
	// public-network throttling). When empty the free endpoint is used.
	IPAPIKey string
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
	if c.FirewallExemptSuperusers == nil {
		v := true
		c.FirewallExemptSuperusers = &v
	}
	c.NodeURL = strings.TrimRight(c.NodeURL, "/")
	c.SeedURL = strings.TrimRight(c.SeedURL, "/")
}

func (c *Config) validate() error {
	if len(c.ClusterSecret) < 16 {
		return errors.New("pbreplication: ClusterSecret must be at least 16 characters")
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

	client *http.Client

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

	r := &Replicator{
		app:         app,
		cfg:         cfg,
		clock:       newHLC(),
		client:      &http.Client{Timeout: 30 * time.Second},
		pushWake:    make(chan struct{}, 1),
		pullWake:    make(chan struct{}, 1),
		applyCh:     make(chan *op, 4096),
		stopCh:      make(chan struct{}),
		pushCursors: map[string]int64{},
		excluded:    map[string]bool{},
	}
	for _, name := range cfg.ExcludeCollections {
		r.excluded[name] = true
	}
	if !*cfg.ReplicateSuperusers {
		r.excluded[core.CollectionNameSuperusers] = true
	}
	r.firewall = newFirewall(r)

	app.OnBootstrap().BindFunc(func(e *core.BootstrapEvent) error {
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

	r.ready.Store(true)
	return nil
}

func (r *Replicator) startBackground() {
	// Now that we are actually serving as a node, set our advertised
	// URL authoritatively (a restart may use a new NodeURL).
	_ = upsertMember(r.app.NonconcurrentDB(), &member{
		NodeID:    r.nodeID,
		URL:       r.cfg.NodeURL,
		Reachable: r.cfg.NodeURL != "",
		LastSeen:  nowStr(),
	})

	if head, err := maxRowID(r.app.DB()); err == nil {
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
		if err := r.bootstrapOrRejoin(); err != nil {
			r.logError("bootstrap failed (will keep retrying via anti-entropy)", err)
		}
	}()
}

func (r *Replicator) shutdown() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()
	if r.nodeID != "" {
		_ = setState(r.app.NonconcurrentDB(), stateHLC, r.clock.Current())
	}
	if r.firewall != nil {
		r.firewall.close()
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

func (r *Replicator) logError(msg string, err error) {
	r.stats.lastError.Store(fmt.Sprintf("%s: %v", msg, err))
	if r.app != nil && r.app.Logger() != nil {
		r.app.Logger().Error("pbreplication: "+msg, slog.String("error", err.Error()))
	}
}

func (r *Replicator) logInfo(msg string, args ...any) {
	if r.app != nil && r.app.Logger() != nil {
		r.app.Logger().Info("pbreplication: "+msg, args...)
	}
}

// wake performs a non-blocking signal on a wake channel.
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
