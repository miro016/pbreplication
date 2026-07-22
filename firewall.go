package pbreplication

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/maxminddb-golang"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
)

// firewallCollection is the hidden system collection holding the rules.
// The collection and field ids are FIXED so that every node creates a
// byte-identical schema and replication converges without conflicts.
const (
	firewallCollection   = "pbr_firewall_rules"
	firewallCollectionID = "pbrfwrules00001"
)

const (
	fwActionAllow = "allow"
	fwActionDeny  = "deny"

	fwScopeApp         = "app"
	fwScopeReplication = "replication"

	fwMatchIP      = "ip"
	fwMatchCIDR    = "cidr"
	fwMatchCountry = "country"
	fwMatchRegion  = "region"
)

// compiledRule is an in-memory, pre-parsed firewall rule.
type compiledRule struct {
	action    string
	scope     string
	matchType string
	value     string
	ip        net.IP     // for fwMatchIP
	ipnet     *net.IPNet // for fwMatchCIDR
}

// firewall enforces IP/CIDR/country/region allow-deny rules on every
// request. Rules live in a regular (superuser-only) PocketBase
// collection, so they replicate across the cluster like any other
// record and can be managed through the standard record API.
type firewall struct {
	r *Replicator

	mu    sync.RWMutex
	rules []compiledRule
	// whitelistMode is set per scope when at least one active allow
	// rule exists for it: anything not explicitly allowed is denied.
	whitelistMode map[string]bool

	geo       *maxminddb.Reader
	geoErr    string
	geoSource string // "file" (Config.GeoIPDBPath) or "embedded"
	// warnings lists configuration problems detected on reload (e.g.
	// country rules without a usable GeoIP database); surfaced in the
	// dashboard so a rule that cannot take effect is never silent.
	warnings    []string
	lastWarnLog string
}

func newFirewall(r *Replicator) *firewall {
	return &firewall{r: r, whitelistMode: map[string]bool{}}
}

func (fw *firewall) close() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.geo != nil {
		fw.geo.Close()
		fw.geo = nil
	}
}

// ---------------------------------------------------------------------
// rules collection

// ensureFirewallCollection creates the rules collection with fixed
// collection/field ids (idempotent and identical on every node).
func (r *Replicator) ensureFirewallCollection(app core.App) error {
	if existing, _ := app.FindCollectionByNameOrId(firewallCollection); existing != nil {
		return nil
	}

	col := core.NewBaseCollection(firewallCollection, firewallCollectionID)
	col.System = true // guard against accidental deletion

	col.Fields.Add(
		&core.SelectField{Id: "pbrf_action", Name: "action", Required: true, MaxSelect: 1,
			Values: []string{fwActionAllow, fwActionDeny}},
		&core.SelectField{Id: "pbrf_scope", Name: "scope", Required: true, MaxSelect: 1,
			Values: []string{fwScopeApp, fwScopeReplication}},
		&core.SelectField{Id: "pbrf_match", Name: "match_type", Required: true, MaxSelect: 1,
			Values: []string{fwMatchIP, fwMatchCIDR, fwMatchCountry, fwMatchRegion}},
		&core.TextField{Id: "pbrf_value", Name: "value", Required: true, Max: 100},
		&core.TextField{Id: "pbrf_note", Name: "note", Max: 500},
		&core.BoolField{Id: "pbrf_active", Name: "active"},
		&core.AutodateField{Id: "pbrf_created", Name: "created", OnCreate: true},
		&core.AutodateField{Id: "pbrf_updated", Name: "updated", OnCreate: true, OnUpdate: true},
	)

	// nil API rules -> superuser only
	return app.Save(col)
}

// bindFirewallHooks recompiles the in-memory ruleset whenever a rule
// record changes (locally or via replication).
func (r *Replicator) bindFirewallHooks(app core.App) {
	reload := func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		r.firewall.reload(e.App)
		return nil
	}
	app.OnRecordAfterCreateSuccess(firewallCollection).BindFunc(reload)
	app.OnRecordAfterUpdateSuccess(firewallCollection).BindFunc(reload)
	app.OnRecordAfterDeleteSuccess(firewallCollection).BindFunc(reload)
}

// reload compiles all active rules into the in-memory ruleset (zero
// per-request DB queries) and lazily opens the GeoIP database.
//
// Country/region rules that cannot possibly match — no GeoIP database
// available, or region rules with a country-only database — are skipped
// entirely ("inert") and reported via warnings instead of being
// compiled. Compiling them anyway would be a lockout trap: an allow
// rule that can never match flips its scope into whitelist mode and
// then blocks every request.
func (fw *firewall) reload(app core.App) {
	records, err := app.FindAllRecords(firewallCollection)
	if err != nil {
		// collection may not exist yet during early bootstrap
		return
	}

	fw.mu.Lock()
	fw.ensureGeoLocked()

	rules := make([]compiledRule, 0, len(records))
	whitelist := map[string]bool{}
	inertGeo, inertRegion := 0, 0

	for _, rec := range records {
		if !rec.GetBool("active") {
			continue
		}
		rule := compiledRule{
			action:    rec.GetString("action"),
			scope:     rec.GetString("scope"),
			matchType: rec.GetString("match_type"),
			value:     strings.TrimSpace(rec.GetString("value")),
		}
		switch rule.matchType {
		case fwMatchIP:
			rule.ip = net.ParseIP(rule.value)
			if rule.ip == nil {
				continue
			}
		case fwMatchCIDR:
			_, ipnet, err := net.ParseCIDR(rule.value)
			if err != nil {
				continue
			}
			rule.ipnet = ipnet
		case fwMatchCountry:
			if fw.geo == nil {
				inertGeo++
				continue
			}
			rule.value = strings.ToUpper(rule.value)
		case fwMatchRegion:
			if fw.geo == nil {
				inertGeo++
				continue
			}
			if !geoHasRegions(fw.geo) {
				inertRegion++
				continue
			}
			rule.value = strings.ToUpper(rule.value)
		default:
			continue
		}
		if rule.action == fwActionAllow {
			whitelist[rule.scope] = true
		}
		rules = append(rules, rule)
	}

	warnings := []string{}
	if inertGeo > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"%d country/region firewall rule(s) are IGNORED: no GeoIP database is available (set GeoIPDBPath or re-enable the embedded database)", inertGeo))
	}
	if inertRegion > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"%d region firewall rule(s) are IGNORED: the loaded GeoIP database (%s) has no region data — set GeoIPDBPath to a city-level .mmdb", inertRegion, fw.geo.Metadata.DatabaseType))
	}
	if fw.geoErr != "" && fw.geo != nil {
		warnings = append(warnings, "custom GeoIP database failed to load ("+fw.geoErr+"), using the embedded one instead")
	}

	fw.rules = rules
	fw.whitelistMode = whitelist
	fw.warnings = warnings

	joined := strings.Join(warnings, "; ")
	logIt := joined != "" && joined != fw.lastWarnLog
	fw.lastWarnLog = joined
	fw.mu.Unlock()

	if logIt {
		fw.r.logWarn("firewall configuration warnings", "warnings", joined)
	}
}

// ensureGeoLocked opens the GeoIP database once: a user-supplied file
// (Config.GeoIPDBPath) takes precedence; otherwise — and as a fallback
// when the file fails to load — the embedded DB-IP Country Lite
// database is used, unless disabled. Requires fw.mu held (write).
func (fw *firewall) ensureGeoLocked() {
	if fw.geo != nil {
		return
	}
	if path := fw.r.cfg.GeoIPDBPath; path != "" {
		geo, err := maxminddb.Open(path)
		if err == nil {
			fw.geo = geo
			fw.geoSource = "file"
			fw.geoErr = ""
			return
		}
		fw.geoErr = err.Error()
	}
	if fw.r.cfg.DisableEmbeddedGeoIP {
		return
	}
	geo, err := openEmbeddedGeoDB()
	if err != nil {
		if fw.geoErr == "" {
			fw.geoErr = err.Error()
		}
		return
	}
	fw.geo = geo
	fw.geoSource = "embedded"
}

// ---------------------------------------------------------------------
// enforcement

// bindMiddleware attaches the firewall to the root router, right after
// PocketBase's auth token loader (so superuser bypass can be checked).
func (fw *firewall) bindMiddleware(se *core.ServeEvent) {
	se.Router.Bind(&hook.Handler[*core.RequestEvent]{
		Id:       "pbreplicationFirewall",
		Priority: apis.DefaultLoadAuthTokenMiddlewarePriority + 6,
		Func:     fw.middleware,
	})
}

func (fw *firewall) middleware(e *core.RequestEvent) error {
	ipStr := e.RealIP()
	ip := net.ParseIP(ipStr)
	method := e.Request.Method
	path := e.Request.URL.Path

	// never lock out loopback
	if ip != nil && ip.IsLoopback() {
		fw.r.trackClient(ipStr, method, path, false)
		return e.Next()
	}

	scope := fwScopeApp
	if strings.HasPrefix(path, "/api/replication/") {
		scope = fwScopeReplication
	}

	// lock-out guard: authenticated superusers bypass app-scope rules
	if scope == fwScopeApp && *fw.r.cfg.FirewallExemptSuperusers && e.HasSuperuserAuth() {
		fw.r.trackClient(ipStr, method, path, false)
		return e.Next()
	}

	if fw.allowed(scope, ip) {
		fw.r.trackClient(ipStr, method, path, false)
		return e.Next()
	}

	fw.r.trackClient(ipStr, method, path, true)
	fw.r.stats.blocked.Add(1)
	// throttled per IP so a scanning client doesn't flood logs/events
	if fw.r.throttleOK("fw:"+ipStr, 10*time.Second) {
		fw.r.logWarn("firewall blocked request", "ip", ipStr, "method", method, "path", path, "scope", scope)
		fw.r.emitEvent(EventFirewallBlock, "firewall blocked request",
			"ip", ipStr, "method", method, "path", path, "scope", scope)
	}
	return e.Error(http.StatusForbidden, "Access denied by firewall rules.", nil)
}

// allowed evaluates the compiled ruleset for a scope and client IP.
func (fw *firewall) allowed(scope string, ip net.IP) bool {
	fw.mu.RLock()
	defer fw.mu.RUnlock()

	if len(fw.rules) == 0 {
		return true
	}

	country, region := fw.lookupGeoLocked(ip)

	allowMatched := false
	for i := range fw.rules {
		rule := &fw.rules[i]
		if rule.scope != scope {
			continue
		}
		if !ruleMatches(rule, ip, country, region) {
			continue
		}
		if rule.action == fwActionDeny {
			return false // explicit deny always wins
		}
		allowMatched = true
	}

	if fw.whitelistMode[scope] {
		return allowMatched
	}
	return true
}

func ruleMatches(rule *compiledRule, ip net.IP, country, region string) bool {
	switch rule.matchType {
	case fwMatchIP:
		return ip != nil && rule.ip.Equal(ip)
	case fwMatchCIDR:
		return ip != nil && rule.ipnet.Contains(ip)
	case fwMatchCountry:
		return country != "" && country == rule.value
	case fwMatchRegion:
		return region != "" && region == rule.value
	}
	return false
}

// lookupGeoLocked resolves the country ISO code and "CC-SUB" region
// code for an IP. Requires fw.mu held (read).
func (fw *firewall) lookupGeoLocked(ip net.IP) (country, region string) {
	if fw.geo == nil || ip == nil {
		return "", ""
	}
	var rec struct {
		Country struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
		Subdivisions []struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"subdivisions"`
	}
	if err := fw.geo.Lookup(ip, &rec); err != nil {
		return "", ""
	}
	country = strings.ToUpper(rec.Country.ISOCode)
	if country != "" && len(rec.Subdivisions) > 0 && rec.Subdivisions[0].ISOCode != "" {
		region = country + "-" + strings.ToUpper(rec.Subdivisions[0].ISOCode)
	}
	return country, region
}

// lookupClientGeo resolves everything the client map can use (country,
// region, city, coordinates) from the loaded GeoIP database. City and
// coordinates stay empty with a country-level database. Returns nil
// when no database is loaded or the IP is unparseable.
func (fw *firewall) lookupClientGeo(ipStr string) *geoResult {
	ip := net.ParseIP(ipStr)

	fw.mu.RLock()
	defer fw.mu.RUnlock()
	if fw.geo == nil || ip == nil {
		return nil
	}

	var rec struct {
		Country struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
		Subdivisions []struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"subdivisions"`
		City struct {
			Names map[string]string `maxminddb:"names"`
		} `maxminddb:"city"`
		Location struct {
			Latitude  float64 `maxminddb:"latitude"`
			Longitude float64 `maxminddb:"longitude"`
		} `maxminddb:"location"`
	}
	if err := fw.geo.Lookup(ip, &rec); err != nil {
		return nil
	}

	res := &geoResult{
		Status:      "fail",
		CountryCode: strings.ToUpper(rec.Country.ISOCode),
		City:        rec.City.Names["en"],
		Lat:         rec.Location.Latitude,
		Lon:         rec.Location.Longitude,
	}
	if res.CountryCode != "" {
		res.Status = "success"
	}
	if len(rec.Subdivisions) > 0 {
		res.Region = strings.ToUpper(rec.Subdivisions[0].ISOCode)
	}
	return res
}

// ---------------------------------------------------------------------
// dashboard summary

type firewallSummary struct {
	RuleCount     int             `json:"rule_count"`
	WhitelistMode map[string]bool `json:"whitelist_mode"`
	GeoIPEnabled  bool            `json:"geoip_enabled"`
	GeoIPSource   string          `json:"geoip_source,omitempty"` // "file" | "embedded"
	GeoIPType     string          `json:"geoip_type,omitempty"`   // e.g. "DBIP-Country-Lite"
	GeoIPError    string          `json:"geoip_error,omitempty"`
	Warnings      []string        `json:"warnings"`
	Blocked       int64           `json:"blocked_total"`
	Collection    string          `json:"collection"`
}

func (r *Replicator) handleFirewallSummary(e *core.RequestEvent) error {
	fw := r.firewall
	fw.mu.RLock()
	summary := &firewallSummary{
		RuleCount:     len(fw.rules),
		WhitelistMode: map[string]bool{},
		GeoIPEnabled:  fw.geo != nil,
		GeoIPSource:   fw.geoSource,
		GeoIPError:    fw.geoErr,
		Warnings:      append([]string{}, fw.warnings...),
		Blocked:       r.stats.blocked.Load(),
		Collection:    firewallCollection,
	}
	if fw.geo != nil {
		summary.GeoIPType = fw.geo.Metadata.DatabaseType
	}
	for k, v := range fw.whitelistMode {
		summary.WhitelistMode[k] = v
	}
	fw.mu.RUnlock()
	return e.JSON(http.StatusOK, summary)
}
