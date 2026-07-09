package pbreplication

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

const testSecret = "test-secret-0123456789"

// newTestNode creates a bootstrapped test app with the replicator
// storage initialized (the OnBootstrap hook can't fire because the test
// app is already bootstrapped, so initStorage is invoked directly).
func newTestNode(t *testing.T, nodeID string) (*tests.TestApp, *Replicator) {
	t.Helper()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)

	r, err := Register(app, Config{
		NodeID:        nodeID,
		NodeURL:       "http://" + nodeID + ".test:8090",
		ClusterSecret: testSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.initStorage(app); err != nil {
		t.Fatal(err)
	}
	return app, r
}

func makeTestCollection(t *testing.T, app core.App, name string) *core.Collection {
	t.Helper()
	col := core.NewBaseCollection(name)
	col.Fields.Add(
		&core.TextField{Name: "title"},
		&core.NumberField{Name: "count"},
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
	)
	if err := app.Save(col); err != nil {
		t.Fatal(err)
	}
	return col
}

func lastOps(t *testing.T, r *Replicator, n int) []*op {
	t.Helper()
	ops, _, err := opsAfterRowID(r.app.DB(), 0, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) < n {
		t.Fatalf("expected at least %d ops, got %d", n, len(ops))
	}
	return ops[len(ops)-n:]
}

// ---------------------------------------------------------------------

func TestHLCOrderingAndObserve(t *testing.T) {
	c := newHLC()
	prev := ""
	for i := 0; i < 1000; i++ {
		cur := c.Now()
		if cur <= prev {
			t.Fatalf("timestamps not strictly increasing: %q then %q", prev, cur)
		}
		prev = cur
	}

	// observing a far-future remote timestamp must push us past it
	future := encodeHLC(uint64(time.Now().Add(time.Hour).UnixMilli()), 7)
	c.Observe(future)
	if next := c.Now(); next <= future {
		t.Fatalf("Now() after Observe(%q) returned %q", future, next)
	}

	p, l, err := decodeHLC(encodeHLC(12345, 42))
	if err != nil || p != 12345 || l != 42 {
		t.Fatalf("decode round-trip failed: %d %d %v", p, l, err)
	}
}

func TestLWWComparator(t *testing.T) {
	if !lwwLess("00a", "n1", "00b", "n1") {
		t.Fatal("lower hlc must lose")
	}
	if lwwLess("00b", "n1", "00a", "n2") {
		t.Fatal("higher hlc must win regardless of node")
	}
	if !lwwLess("00a", "n1", "00a", "n2") {
		t.Fatal("equal hlc must tiebreak by node id")
	}
	if lwwLess("00a", "n2", "00a", "n2") {
		t.Fatal("identical write must not supersede itself")
	}
}

func TestCaptureProducesOplog(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	col := makeTestCollection(t, app, "posts")

	rec := core.NewRecord(col)
	rec.Set("title", "hello")
	rec.Set("count", 3)
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}

	ops := lastOps(t, r, 1)
	o := ops[len(ops)-1]
	if o.Type != opUpsert || o.RecordID != rec.Id || o.ColName != "posts" || o.SrcNode != r.nodeID {
		t.Fatalf("unexpected op: %+v", o)
	}

	var payload map[string]any
	if err := json.Unmarshal(o.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["title"] != "hello" {
		t.Fatalf("payload title = %v", payload["title"])
	}

	ver, err := getVersion(app.DB(), col.Id, rec.Id)
	if err != nil || ver == nil {
		t.Fatalf("missing version row: %v", err)
	}
	if ver.HLC != o.HLC || ver.Deleted {
		t.Fatalf("bad version row: %+v", ver)
	}

	// deleting must produce a tombstone op + version
	if err := app.Delete(rec); err != nil {
		t.Fatal(err)
	}
	o = lastOps(t, r, 1)[0]
	if o.Type != opDelete || o.RecordID != rec.Id {
		t.Fatalf("expected delete op, got %+v", o)
	}
	ver, _ = getVersion(app.DB(), col.Id, rec.Id)
	if ver == nil || !ver.Deleted {
		t.Fatalf("expected tombstone version, got %+v", ver)
	}
}

func TestReplicationRoundTrip(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	appB, rB := newTestNode(t, "nodeB0000000001")

	// 1. schema replicates: create collection on A, apply its op on B
	makeTestCollection(t, appA, "posts")
	colOp := lastOps(t, rA, 1)[0]
	if colOp.Type != opColUpsert {
		t.Fatalf("expected col_upsert, got %+v", colOp)
	}
	if err := rB.applyOp(colOp); err != nil {
		t.Fatal(err)
	}
	colB, err := appB.FindCollectionByNameOrId("posts")
	if err != nil || colB == nil {
		t.Fatalf("collection not replicated: %v", err)
	}

	// 2. record create replicates AND fires user hooks on B
	hookFired := 0
	appB.OnRecordAfterCreateSuccess("posts").BindFunc(func(e *core.RecordEvent) error {
		hookFired++
		return e.Next()
	})

	colA, _ := appA.FindCollectionByNameOrId("posts")
	rec := core.NewRecord(colA)
	rec.Set("title", "from A")
	rec.Set("count", 42)
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	recOp := lastOps(t, rA, 1)[0]
	if err := rB.applyOp(recOp); err != nil {
		t.Fatal(err)
	}

	got, err := appB.FindRecordById(colB, rec.Id)
	if err != nil {
		t.Fatalf("record not replicated: %v", err)
	}
	if got.GetString("title") != "from A" || got.GetFloat("count") != 42 {
		t.Fatalf("replicated record mismatch: %v / %v", got.GetString("title"), got.GetFloat("count"))
	}
	if got.GetString("created") != rec.GetString("created") {
		t.Fatalf("autodate not preserved: %q vs %q", got.GetString("created"), rec.GetString("created"))
	}
	if hookFired != 1 {
		t.Fatalf("user hook fired %d times on B", hookFired)
	}

	// 3. update on A replicates to B
	rec.Set("title", "updated")
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}
	got, _ = appB.FindRecordById(colB, rec.Id)
	if got.GetString("title") != "updated" {
		t.Fatalf("update not applied: %q", got.GetString("title"))
	}

	// 4. delete on A replicates to B
	if err := appA.Delete(rec); err != nil {
		t.Fatal(err)
	}
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := appB.FindRecordById(colB, rec.Id); err == nil {
		t.Fatal("record still exists on B after replicated delete")
	}
	ver, _ := getVersion(appB.DB(), recOp.ColID, rec.Id)
	if ver == nil || !ver.Deleted {
		t.Fatalf("expected tombstone on B, got %+v", ver)
	}

	// 5. applying our own echoed op is a no-op
	if err := rA.applyOp(recOp); err != nil {
		t.Fatal(err)
	}
}

func TestLWWGateRejectsOlderWrites(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	_, rB := newTestNode(t, "nodeB0000000001")
	appB := rB.app

	makeTestCollection(t, appA, "posts")
	if err := rB.applyOp(lastOps(t, rA, 1)[0]); err != nil {
		t.Fatal(err)
	}

	colA, _ := appA.FindCollectionByNameOrId("posts")
	rec := core.NewRecord(colA)
	rec.Set("title", "v1")
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	oldOp := lastOps(t, rA, 1)[0]

	rec.Set("title", "v2")
	if err := appA.Save(rec); err != nil {
		t.Fatal(err)
	}
	newOp := lastOps(t, rA, 1)[0]

	// apply newer first, then the stale one
	if err := rB.applyOp(newOp); err != nil {
		t.Fatal(err)
	}
	if err := rB.applyOp(oldOp); err != nil {
		t.Fatal(err)
	}

	colB, _ := appB.FindCollectionByNameOrId("posts")
	got, err := appB.FindRecordById(colB, rec.Id)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetString("title") != "v2" {
		t.Fatalf("stale write clobbered newer one: %q", got.GetString("title"))
	}

	// a delete with an older HLC must not resurrect/remove a newer write
	staleDelete := &op{
		SrcNode: "nodeC0000000001", SrcSeq: 1, HLC: encodeHLC(1, 0),
		Type: opDelete, ColID: oldOp.ColID, ColName: "posts", RecordID: rec.Id,
	}
	if err := rB.applyOp(staleDelete); err != nil {
		t.Fatal(err)
	}
	if _, err := appB.FindRecordById(colB, rec.Id); err != nil {
		t.Fatal("stale delete removed a newer record")
	}
}

func TestCollectionOpIdempotence(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")
	_, rB := newTestNode(t, "nodeB0000000001")

	makeTestCollection(t, appA, "posts")
	colOp := lastOps(t, rA, 1)[0]

	if err := rB.applyOp(colOp); err != nil {
		t.Fatal(err)
	}
	// identical op again (e.g. the same migration ran on another node)
	dup := *colOp
	dup.SrcNode = "nodeC0000000001"
	dup.SrcSeq = 1
	dup.HLC = rB.clock.Now() // newer, but identical content
	if err := rB.applyOp(&dup); err != nil {
		t.Fatalf("idempotent re-apply failed: %v", err)
	}

	cols, err := rB.app.FindAllCollections()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, c := range cols {
		if c.Name == "posts" {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("expected exactly 1 posts collection, got %d", seen)
	}
}

func TestHMACAuth(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	body := []byte(`{"x":1}`)
	req := httptest.NewRequest("POST", "http://peer/api/replication/ops", strings.NewReader(string(body)))
	r.signRequest(req, body)

	nodeID, err := r.verifyAuth(req, body)
	if err != nil || nodeID != r.nodeID {
		t.Fatalf("valid request rejected: %v (%s)", err, nodeID)
	}

	// tampered body
	if _, err := r.verifyAuth(req, []byte(`{"x":2}`)); err == nil {
		t.Fatal("tampered body accepted")
	}

	// wrong secret
	other := &Replicator{cfg: Config{ClusterSecret: "another-secret-0123456789"}, nodeID: "x"}
	if _, err := other.verifyAuth(req, body); err == nil {
		t.Fatal("wrong secret accepted")
	}

	// stale timestamp
	staleReq := httptest.NewRequest("POST", "http://peer/api/replication/ops", strings.NewReader(string(body)))
	ts := fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix())
	mac := signPayload(testSecret, r.nodeID, ts, "POST", staleReq.URL.Path, body)
	staleReq.Header.Set("Authorization", fmt.Sprintf("PBR %s.%s.%s", r.nodeID, ts, mac))
	if _, err := r.verifyAuth(staleReq, body); err == nil {
		t.Fatal("stale timestamp accepted")
	}
}

func TestVectorAndPull(t *testing.T) {
	appA, rA := newTestNode(t, "nodeA0000000001")

	makeTestCollection(t, appA, "posts")
	colA, _ := appA.FindCollectionByNameOrId("posts")
	for i := 0; i < 5; i++ {
		rec := core.NewRecord(colA)
		rec.Set("title", fmt.Sprintf("r%d", i))
		if err := appA.Save(rec); err != nil {
			t.Fatal(err)
		}
	}

	// 1 collection op + 5 record ops
	vec, err := rA.currentVector()
	if err != nil {
		t.Fatal(err)
	}
	if vec[rA.nodeID] != 6 {
		t.Fatalf("expected local seq 6, got %d", vec[rA.nodeID])
	}

	// a fresh peer pulls everything
	ops, snap, err := opsAfterVector(appA.DB(), map[string]int64{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if snap {
		t.Fatal("unexpected snapshot_required")
	}
	if len(ops) != 6 {
		t.Fatalf("expected 6 ops, got %d", len(ops))
	}

	// a caught-up peer pulls nothing
	ops, _, err = opsAfterVector(appA.DB(), map[string]int64{rA.nodeID: 6}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected 0 ops, got %d", len(ops))
	}
}

func TestCompactionGC(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	r.cfg.CompactionInterval = time.Nanosecond // everything is past grace
	r.cfg.TombstoneRetention = time.Nanosecond // everything is expired

	col := makeTestCollection(t, app, "posts")

	rec := core.NewRecord(col)
	rec.Set("title", "a")
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}
	rec.Set("title", "b")
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}

	rec2 := core.NewRecord(col)
	rec2.Set("title", "gone")
	if err := app.Save(rec2); err != nil {
		t.Fatal(err)
	}
	if err := app.Delete(rec2); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond) // created timestamps have 1s resolution

	if err := r.compact(); err != nil {
		t.Fatal(err)
	}

	ops, _, _ := opsAfterRowID(app.DB(), 0, 1000)
	for _, o := range ops {
		if o.RecordID == rec.Id && o.Type == opUpsert {
			var p map[string]any
			_ = json.Unmarshal(o.Payload, &p)
			if p["title"] != "b" {
				t.Fatalf("superseded op survived compaction: %v", p["title"])
			}
		}
		if o.Type == opDelete {
			t.Fatal("expired tombstone op survived compaction")
		}
	}

	// tombstone version row purged
	ver, _ := getVersion(app.DB(), col.Id, rec2.Id)
	if ver != nil {
		t.Fatalf("expired version tombstone survived: %+v", ver)
	}
	// live version row stays
	ver, _ = getVersion(app.DB(), col.Id, rec.Id)
	if ver == nil {
		t.Fatal("live version row was wrongly purged")
	}

	// tombstone horizon recorded -> stale peers get snapshot_required
	_, snap, err := opsAfterVector(app.DB(), map[string]int64{r.nodeID: 0}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !snap {
		t.Fatal("expected snapshot_required for a peer behind the tombstone horizon")
	}
}

func TestFirewallMatcher(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")
	fw := r.firewall

	compile := func(rules ...compiledRule) {
		fw.mu.Lock()
		fw.rules = rules
		fw.whitelistMode = map[string]bool{}
		for _, rule := range rules {
			if rule.action == fwActionAllow {
				fw.whitelistMode[rule.scope] = true
			}
		}
		fw.mu.Unlock()
	}
	mustCIDR := func(s string) *net.IPNet {
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// no rules: everything allowed
	compile()
	if !fw.allowed(fwScopeApp, net.ParseIP("203.0.113.7")) {
		t.Fatal("empty ruleset must allow")
	}

	// blacklist mode: single deny ip
	compile(compiledRule{action: fwActionDeny, scope: fwScopeApp, matchType: fwMatchIP, ip: net.ParseIP("203.0.113.7")})
	if fw.allowed(fwScopeApp, net.ParseIP("203.0.113.7")) {
		t.Fatal("denied ip allowed")
	}
	if !fw.allowed(fwScopeApp, net.ParseIP("203.0.113.8")) {
		t.Fatal("other ip blocked in blacklist mode")
	}
	if !fw.allowed(fwScopeReplication, net.ParseIP("203.0.113.7")) {
		t.Fatal("app-scope rule leaked into replication scope")
	}

	// whitelist mode: allow a CIDR on the replication scope
	compile(compiledRule{action: fwActionAllow, scope: fwScopeReplication, matchType: fwMatchCIDR, ipnet: mustCIDR("10.0.0.0/8")})
	if !fw.allowed(fwScopeReplication, net.ParseIP("10.1.2.3")) {
		t.Fatal("whitelisted range blocked")
	}
	if fw.allowed(fwScopeReplication, net.ParseIP("192.168.1.1")) {
		t.Fatal("non-whitelisted ip allowed in whitelist mode")
	}
	if !fw.allowed(fwScopeApp, net.ParseIP("192.168.1.1")) {
		t.Fatal("replication whitelist leaked into app scope")
	}

	// deny wins over allow
	compile(
		compiledRule{action: fwActionAllow, scope: fwScopeApp, matchType: fwMatchCIDR, ipnet: mustCIDR("10.0.0.0/8")},
		compiledRule{action: fwActionDeny, scope: fwScopeApp, matchType: fwMatchIP, ip: net.ParseIP("10.0.0.5")},
	)
	if fw.allowed(fwScopeApp, net.ParseIP("10.0.0.5")) {
		t.Fatal("explicit deny must beat allow")
	}
	if !fw.allowed(fwScopeApp, net.ParseIP("10.0.0.6")) {
		t.Fatal("allowed range blocked")
	}
}

func TestClientTrackingAndGeoCache(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")

	// buffered counting -> batched flush
	r.trackClient("127.0.0.1", "GET", "/", false)
	r.trackClient("127.0.0.1", "GET", "/", false)
	r.trackClient("8.8.8.8", "GET", "/api/collections/users/records", false)
	r.trackClient("8.8.8.8", "POST", "/api/admins/auth", true)
	r.flushClients()

	var loop clientRow
	if err := app.DB().NewQuery(`SELECT * FROM _repl_client_ips WHERE ip = '127.0.0.1'`).One(&loop); err != nil {
		t.Fatal(err)
	}
	if loop.Requests != 2 || loop.Blocked != 0 || loop.GeoStatus != geoPrivate {
		t.Fatalf("bad loopback row: %+v", loop)
	}

	var pub clientRow
	if err := app.DB().NewQuery(`SELECT * FROM _repl_client_ips WHERE ip = '8.8.8.8'`).One(&pub); err != nil {
		t.Fatal(err)
	}
	if pub.Requests != 2 || pub.Blocked != 1 || pub.GeoStatus != geoPending {
		t.Fatalf("bad public row: %+v", pub)
	}

	// counters accumulate across flushes
	r.trackClient("8.8.8.8", "POST", "/api/admins/auth", true)
	r.flushClients()
	_ = app.DB().NewQuery(`SELECT * FROM _repl_client_ips WHERE ip = '8.8.8.8'`).One(&pub)
	if pub.Requests != 3 || pub.Blocked != 2 {
		t.Fatalf("counters did not accumulate: %+v", pub)
	}

	// per-path tracking
	var paths []*clientPathRow
	if err := app.DB().NewQuery(`SELECT method, path, count, blocked, last_seen
		FROM _repl_client_paths WHERE ip = '8.8.8.8' ORDER BY count DESC`).All(&paths); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 tracked paths, got %d", len(paths))
	}
	var authPath *clientPathRow
	for _, p := range paths {
		if p.Path == "/api/admins/auth" {
			authPath = p
		}
	}
	if authPath == nil || authPath.Method != "POST" || authPath.Count != 2 || authPath.Blocked != 2 {
		t.Fatalf("bad auth path row: %+v", authPath)
	}

	// geolocation: exactly one lookup per IP, result cached
	calls := 0
	r.geoLookup = func(ip string) (*geoResult, error) {
		calls++
		if ip != "8.8.8.8" {
			t.Fatalf("unexpected lookup for %s", ip)
		}
		return &geoResult{Status: "success", CountryCode: "US", Region: "CA", City: "Mountain View", Lat: 37.4, Lon: -122.1}, nil
	}
	if !r.geoStep() {
		t.Fatal("expected a pending lookup")
	}
	if r.geoStep() {
		t.Fatal("no lookup should remain pending (loopback is private, 8.8.8.8 cached)")
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 ip-api call, got %d", calls)
	}

	_ = app.DB().NewQuery(`SELECT * FROM _repl_client_ips WHERE ip = '8.8.8.8'`).One(&pub)
	if pub.GeoStatus != geoOK || pub.Country != "US" || pub.Region != "US-CA" || pub.Lat == 0 {
		t.Fatalf("geo result not cached: %+v", pub)
	}

	// GC keeps fresh rows
	if err := gcClients(app.NonconcurrentDB(), time.Hour); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = app.DB().NewQuery(`SELECT COUNT(*) FROM _repl_client_ips`).Row(&n)
	if n != 2 {
		t.Fatalf("gc removed fresh rows: %d left", n)
	}
}

func TestFirewallCollectionCreated(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")

	col, err := app.FindCollectionByNameOrId(firewallCollection)
	if err != nil || col == nil {
		t.Fatalf("firewall collection missing: %v", err)
	}
	if col.Id != firewallCollectionID {
		t.Fatalf("firewall collection id must be fixed, got %q", col.Id)
	}

	// creating a rule record recompiles the ruleset
	rec := core.NewRecord(col)
	rec.Set("action", "deny")
	rec.Set("scope", "app")
	rec.Set("match_type", "ip")
	rec.Set("value", "203.0.113.9")
	rec.Set("active", true)
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}

	if r.firewall.allowed(fwScopeApp, net.ParseIP("203.0.113.9")) {
		t.Fatal("rule record did not take effect")
	}
}

// ---------------------------------------------------------------------
// deferred migrations (fresh node joining an existing cluster)

// stashAppMigrations snapshots the global core.AppMigrations registry
// and restores it when the test finishes, so tests can register fake
// migrations without leaking into each other.
func stashAppMigrations(t *testing.T) {
	t.Helper()
	orig := core.AppMigrations
	core.AppMigrations = core.MigrationsList{}
	t.Cleanup(func() { core.AppMigrations = orig })
}

// newTestNodeCfg is newTestNode with a caller-provided config, wired to
// an app created beforehand (so fake migrations can be registered after
// tests.NewTestApp ran its own migration pass but before initStorage).
func newTestNodeCfg(t *testing.T, app *tests.TestApp, cfg Config) *Replicator {
	t.Helper()
	r, err := Register(app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.initStorage(app); err != nil {
		t.Fatal(err)
	}
	return r
}

func newTestAppOnly(t *testing.T) *tests.TestApp {
	t.Helper()
	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)
	return app
}

func migrationApplied(t *testing.T, app core.App, file string) bool {
	t.Helper()
	var n int
	err := app.DB().NewQuery(`SELECT COUNT(*) FROM _migrations WHERE file = {:f}`).
		Bind(dbx.Params{"f": file}).Row(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func TestDeferAppMigrationsDecision(t *testing.T) {
	noop := func(txApp core.App) error { return nil }

	register2 := func() {
		core.AppMigrations.Register(noop, nil, "m1.go")
		core.AppMigrations.Register(noop, nil, "m2.go")
	}

	// fresh node with a seed: migrations must be deferred
	t.Run("fresh with seed", func(t *testing.T) {
		stashAppMigrations(t)
		app := newTestAppOnly(t)
		register2()
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       "http://seed.test:8090",
			ClusterSecret: testSecret,
		})
		if len(core.AppMigrations.Items()) != 0 {
			t.Fatal("global registry not emptied")
		}
		if !r.migrationsDeferred || len(r.deferredMigrations.Items()) != 2 {
			t.Fatalf("expected 2 deferred migrations, got %d (deferred=%v)",
				len(r.deferredMigrations.Items()), r.migrationsDeferred)
		}
	})

	// first node of a new cluster: untouched
	t.Run("no seed", func(t *testing.T) {
		stashAppMigrations(t)
		app := newTestAppOnly(t)
		register2()
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			ClusterSecret: testSecret,
		})
		if len(core.AppMigrations.Items()) != 2 || r.migrationsDeferred {
			t.Fatal("migrations must not be deferred without a seed")
		}
	})

	// already bootstrapped: untouched
	t.Run("already bootstrapped", func(t *testing.T) {
		stashAppMigrations(t)
		app := newTestAppOnly(t)
		if err := createTables(app); err != nil {
			t.Fatal(err)
		}
		if err := setState(app.NonconcurrentDB(), stateBootstrapDone, nowStr()); err != nil {
			t.Fatal(err)
		}
		register2()
		r := newTestNodeCfg(t, app, Config{
			NodeID:        "nodeA0000000001",
			SeedURL:       "http://seed.test:8090",
			ClusterSecret: testSecret,
		})
		if len(core.AppMigrations.Items()) != 2 || r.migrationsDeferred {
			t.Fatal("migrations must not be deferred after bootstrap_done")
		}
	})

	// deferral disabled: untouched
	t.Run("opt-out", func(t *testing.T) {
		stashAppMigrations(t)
		app := newTestAppOnly(t)
		register2()
		off := false
		r := newTestNodeCfg(t, app, Config{
			NodeID:                     "nodeA0000000001",
			SeedURL:                    "http://seed.test:8090",
			ClusterSecret:              testSecret,
			DeferMigrationsUntilSynced: &off,
		})
		if len(core.AppMigrations.Items()) != 2 || r.migrationsDeferred {
			t.Fatal("migrations must not be deferred when opted out")
		}
	})
}

func TestAppliedMigrationsListAndWireShape(t *testing.T) {
	app := newTestAppOnly(t)

	files, err := listAppliedMigrations(app.DB())
	if err != nil {
		t.Fatal(err)
	}
	if files == nil {
		t.Fatal("listAppliedMigrations must return a non-nil slice")
	}
	if len(files) == 0 {
		t.Fatal("expected the test app's own migrations in _migrations")
	}

	// nil vs [] must be distinguishable on the wire (no omitempty)
	b, err := json.Marshal(&snapshotMeta{AppliedMigrations: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"applied_migrations":[]`) {
		t.Fatalf("empty applied_migrations must serialize as []: %s", b)
	}
}

func TestImportAndRunDeferredMigrations(t *testing.T) {
	stashAppMigrations(t)
	app := newTestAppOnly(t)

	m1Ran, m2Ran := false, false
	core.AppMigrations.Register(func(txApp core.App) error {
		m1Ran = true
		return nil
	}, nil, "m1.go")
	core.AppMigrations.Register(func(txApp core.App) error {
		m2Ran = true
		col := core.NewBaseCollection("dm_two")
		col.Fields.Add(&core.TextField{Name: "title"})
		if err := txApp.Save(col); err != nil {
			return err
		}
		rec := core.NewRecord(col)
		rec.Set("title", "seeded")
		return txApp.Save(rec)
	}, nil, "m2.go")

	r := newTestNodeCfg(t, app, Config{
		NodeID:        "nodeA0000000001",
		SeedURL:       "http://seed.test:8090",
		ClusterSecret: testSecret,
	})
	if !r.migrationsDeferred {
		t.Fatal("expected deferred migrations")
	}

	// the cluster applied m1 (plus files this node doesn't know)
	if err := r.importClusterMigrations([]string{"m1.go", "unknown_elsewhere.go"}); err != nil {
		t.Fatal(err)
	}
	if !migrationApplied(t, app, "m1.go") {
		t.Fatal("m1.go must be marked applied")
	}
	if migrationApplied(t, app, "m2.go") || migrationApplied(t, app, "unknown_elsewhere.go") {
		t.Fatal("only deferred files reported by the seed may be imported")
	}

	if err := r.runDeferredMigrations(); err != nil {
		t.Fatal(err)
	}
	if m1Ran {
		t.Fatal("m1 was applied cluster-wide and must not run")
	}
	if !m2Ran {
		t.Fatal("m2 is new and must run after the sync")
	}
	if !migrationApplied(t, app, "m2.go") {
		t.Fatal("m2.go must be recorded as applied")
	}
	if r.migrationsDeferred {
		t.Fatal("deferred state must be cleared")
	}

	// m2's writes must be captured for replication
	ops := lastOps(t, r, 2)
	if ops[0].ColName != "dm_two" || ops[1].ColName != "dm_two" {
		t.Fatalf("post-sync migration writes not captured: %+v", ops)
	}

	// nil applied list (old seed): everything is assumed applied
	t.Run("nil applied list", func(t *testing.T) {
		stashAppMigrations(t)
		app2 := newTestAppOnly(t)
		ran := false
		core.AppMigrations.Register(func(txApp core.App) error {
			ran = true
			return nil
		}, nil, "m3.go")
		r2 := newTestNodeCfg(t, app2, Config{
			NodeID:        "nodeB0000000001",
			SeedURL:       "http://seed.test:8090",
			ClusterSecret: testSecret,
		})
		if err := r2.importClusterMigrations(nil); err != nil {
			t.Fatal(err)
		}
		if err := r2.runDeferredMigrations(); err != nil {
			t.Fatal(err)
		}
		if ran {
			t.Fatal("with an unreported history all deferred migrations are assumed applied")
		}
		if !migrationApplied(t, app2, "m3.go") {
			t.Fatal("m3.go must be marked applied")
		}
	})
}

// fakeSeed serves just enough of the replication API for a fresh node
// to bootstrap against it.
func fakeSeed(t *testing.T, meta any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/replication/join", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(&joinResponse{NodeID: "seed000000000001", URLVerified: true})
	})
	mux.HandleFunc("/api/replication/snapshot/meta", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/replication/snapshot/records", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(&snapshotRecordsPage{})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestBootstrapDefersUntilSnapshot(t *testing.T) {
	// export a collection from a stand-in "seed" app
	seedApp := newTestAppOnly(t)
	seedCol := makeTestCollection(t, seedApp, "seed_posts")
	colJSON, err := exportCollectionJSON(seedCol)
	if err != nil {
		t.Fatal(err)
	}

	srv := fakeSeed(t, &snapshotMeta{
		NodeID:            "seed000000000001",
		Collections:       []json.RawMessage{colJSON},
		Vector:            map[string]int64{"seed000000000001": 1},
		AppliedMigrations: []string{"m1.go"},
	})

	stashAppMigrations(t)
	app := newTestAppOnly(t)
	m1Ran, m2Ran := false, false
	core.AppMigrations.Register(func(txApp core.App) error { m1Ran = true; return nil }, nil, "m1.go")
	core.AppMigrations.Register(func(txApp core.App) error { m2Ran = true; return nil }, nil, "m2.go")

	r := newTestNodeCfg(t, app, Config{
		NodeID:        "nodeB0000000001",
		SeedURL:       srv.URL,
		ClusterSecret: testSecret,
	})

	if err := r.bootstrapOrRejoin(); err != nil {
		t.Fatal(err)
	}

	if _, err := app.FindCollectionByNameOrId("seed_posts"); err != nil {
		t.Fatal("snapshot collection missing after bootstrap")
	}
	if m1Ran {
		t.Fatal("m1 was applied on the cluster and must not run")
	}
	if !m2Ran {
		t.Fatal("m2 must run after the snapshot")
	}
	if !migrationApplied(t, app, "m1.go") || !migrationApplied(t, app, "m2.go") {
		t.Fatal("both migrations must be recorded in _migrations")
	}
	if done, _ := getState(app.DB(), stateBootstrapDone); done == "" {
		t.Fatal("bootstrap_done must be set")
	}

	// seed running an older library version: applied_migrations absent
	t.Run("seed without migration history", func(t *testing.T) {
		oldSrv := fakeSeed(t, map[string]any{
			"node_id":     "seed000000000001",
			"collections": []json.RawMessage{colJSON},
			"vector":      map[string]int64{"seed000000000001": 1},
		})

		stashAppMigrations(t)
		app2 := newTestAppOnly(t)
		ran := false
		core.AppMigrations.Register(func(txApp core.App) error { ran = true; return nil }, nil, "m1.go")

		r2 := newTestNodeCfg(t, app2, Config{
			NodeID:        "nodeC0000000001",
			SeedURL:       oldSrv.URL,
			ClusterSecret: testSecret,
		})
		if err := r2.bootstrapOrRejoin(); err != nil {
			t.Fatal(err)
		}
		if ran {
			t.Fatal("without a reported history all deferred migrations are assumed applied")
		}
		if !migrationApplied(t, app2, "m1.go") {
			t.Fatal("m1.go must be marked applied")
		}
	})
}
