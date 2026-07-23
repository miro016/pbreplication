package pbreplication

import (
	"net"
	"strings"
	"testing"

	"github.com/pocketbase/pocketbase/core"
)

func addFirewallRule(t *testing.T, app core.App, action, scope, matchType, value string) {
	t.Helper()
	col, err := app.FindCollectionByNameOrId(firewallCollection)
	if err != nil {
		t.Fatal(err)
	}
	rec := core.NewRecord(col)
	rec.Set("action", action)
	rec.Set("scope", scope)
	rec.Set("match_type", matchType)
	rec.Set("value", value)
	rec.Set("active", true)
	if err := app.Save(rec); err != nil {
		t.Fatal(err)
	}
}

// Regression test: an "allow one country" rule must actually restrict
// with zero GeoIP configuration, using the embedded DB-IP database.
func TestEmbeddedGeoDBCountryRules(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	fw := r.firewall

	fw.mu.RLock()
	geo, src := fw.geo, fw.geoSource
	fw.mu.RUnlock()
	if geo == nil || src != "embedded" {
		t.Fatalf("embedded geo db not loaded (source %q)", src)
	}

	addFirewallRule(t, app, "allow", "app", "country", "sk")

	if !fw.allowed(fwScopeApp, net.ParseIP("178.143.168.102")) {
		t.Fatal("IP from the allowed country (SK) was blocked")
	}
	if fw.allowed(fwScopeApp, net.ParseIP("8.8.8.8")) {
		t.Fatal("IP outside the allowed country passed the whitelist")
	}
	if !fw.allowed(fwScopeReplication, net.ParseIP("8.8.8.8")) {
		t.Fatal("app-scope country whitelist leaked into replication scope")
	}

	fw.mu.RLock()
	warns := len(fw.warnings)
	fw.mu.RUnlock()
	if warns != 0 {
		t.Fatalf("unexpected warnings with a working geo db: %v", fw.warnings)
	}
}

// Without any GeoIP database, geo rules must be inert (skipped, no
// whitelist flip, no lockout) and reported as warnings.
func TestGeoRulesInertWithoutDB(t *testing.T) {
	app := newTestAppOnly(t)
	r := newTestNodeCfg(t, app, Config{
		NodeID:               "nodeA0000000001",
		NodeURL:              "http://nodeA.test:8090",
		ClusterSecret:        testSecret,
		DisableEmbeddedGeoIP: true,
	})
	fw := r.firewall

	addFirewallRule(t, app, "allow", "app", "country", "SK")

	if !fw.allowed(fwScopeApp, net.ParseIP("8.8.8.8")) {
		t.Fatal("inert country rule must not block anyone")
	}

	fw.mu.RLock()
	defer fw.mu.RUnlock()
	if len(fw.rules) != 0 || fw.whitelistMode[fwScopeApp] {
		t.Fatalf("geo rule compiled without a geo db: rules=%d whitelist=%v", len(fw.rules), fw.whitelistMode)
	}
	if len(fw.warnings) != 1 || !strings.Contains(fw.warnings[0], "IGNORED") {
		t.Fatalf("expected an ignored-rules warning, got %v", fw.warnings)
	}
}

// Region rules need subdivision data the bundled country-level
// database doesn't have — they must be inert with a warning, while
// country rules keep working.
func TestRegionRulesInertWithCountryDB(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	fw := r.firewall

	addFirewallRule(t, app, "allow", "app", "region", "SK-BL")

	if !fw.allowed(fwScopeApp, net.ParseIP("8.8.8.8")) {
		t.Fatal("inert region rule must not block anyone")
	}

	fw.mu.RLock()
	warnings := append([]string{}, fw.warnings...)
	whitelist := fw.whitelistMode[fwScopeApp]
	fw.mu.RUnlock()
	if whitelist {
		t.Fatal("inert region rule flipped whitelist mode")
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "region") {
		t.Fatalf("expected a region-rules warning, got %v", warnings)
	}

	// a country rule alongside it still enforces
	addFirewallRule(t, app, "allow", "app", "country", "SK")
	if fw.allowed(fwScopeApp, net.ParseIP("8.8.8.8")) {
		t.Fatal("country whitelist not enforced")
	}
	if !fw.allowed(fwScopeApp, net.ParseIP("178.143.168.102")) {
		t.Fatal("allowed country blocked")
	}
}

// Client-map geolocation resolves locally from the embedded database
// by default — no external calls.
func TestClientGeoFromLocalDB(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")

	if r.useIPAPI() {
		t.Fatal("ip-api must be opt-in")
	}

	res, err := r.lookupGeoMMDB("178.143.168.102")
	if err != nil || res.Status != "success" || res.CountryCode != "SK" {
		t.Fatalf("local lookup failed: %+v err=%v", res, err)
	}

	// full pipeline: track -> flush -> geoStep caches the result
	r.geoLookup = r.lookupGeoMMDB
	r.trackClient("8.8.8.8", "GET", "/", false)
	r.flushClients()
	if !r.geoStep() {
		t.Fatal("expected a pending lookup")
	}
	var row clientRow
	if err := app.DB().NewQuery(`SELECT * FROM _repl_client_ips WHERE ip = '8.8.8.8'`).One(&row); err != nil {
		t.Fatal(err)
	}
	if row.GeoStatus != geoOK || row.Country != "US" {
		t.Fatalf("client not geolocated from local db: %+v", row)
	}
}
