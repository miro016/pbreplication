package pbreplication

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// Client IP tracking + geolocation for the dashboard map.
//
// Every request's client IP is counted in memory (no per-request DB
// write) and flushed to the node-local _repl_client_ips table in
// batches. Each NEW public IP is geolocated exactly once via
// ip-api.com (free endpoint, rate-limited well below its 45 req/min
// cap) and the result is cached permanently in the table.

// geo_status values
const (
	geoPending = ""        // not looked up yet
	geoOK      = "ok"      // located
	geoPrivate = "private" // loopback/LAN address, nothing to locate
	geoFailed  = "failed"  // ip-api answered "fail" (reserved range etc.)
)

const (
	geoLookupInterval = 2 * time.Second // ~30 req/min, under ip-api's 45/min
	clientFlushBatch  = 512
	maxTrackedClients = 10000
)

type clientCounter struct {
	requests atomic.Int64
	blocked  atomic.Int64
}

type clientRow struct {
	IP        string  `db:"ip" json:"ip"`
	FirstSeen string  `db:"first_seen" json:"first_seen"`
	LastSeen  string  `db:"last_seen" json:"last_seen"`
	Requests  int64   `db:"requests" json:"requests"`
	Blocked   int64   `db:"blocked" json:"blocked"`
	Country   string  `db:"country" json:"country"`
	Region    string  `db:"region" json:"region"`
	City      string  `db:"city" json:"city"`
	Lat       float64 `db:"lat" json:"lat"`
	Lon       float64 `db:"lon" json:"lon"`
	GeoStatus string  `db:"geo_status" json:"geo_status"`
}

// trackClient counts a request from an IP; called from the firewall
// middleware on every request, so it must stay cheap (two atomic adds).
func (r *Replicator) trackClient(ip string, blocked bool) {
	if ip == "" {
		return
	}
	v, _ := r.clientCounts.LoadOrStore(ip, &clientCounter{})
	c := v.(*clientCounter)
	c.requests.Add(1)
	if blocked {
		c.blocked.Add(1)
	}
}

// flushClients writes the buffered per-IP counters to the table.
// Called from the anti-entropy loop (every SyncInterval).
func (r *Replicator) flushClients() {
	db := r.app.NonconcurrentDB()
	now := nowStr()
	flushed := 0

	r.clientCounts.Range(func(key, v any) bool {
		ip := key.(string)
		c := v.(*clientCounter)
		req := c.requests.Swap(0)
		blk := c.blocked.Swap(0)
		if req == 0 && blk == 0 {
			r.clientCounts.Delete(ip) // idle since last flush
			return true
		}

		status := geoPending
		if parsed := net.ParseIP(ip); parsed == nil ||
			parsed.IsLoopback() || parsed.IsPrivate() ||
			parsed.IsLinkLocalUnicast() || parsed.IsUnspecified() {
			status = geoPrivate
		}

		_, err := db.NewQuery(`INSERT INTO _repl_client_ips
			(ip, first_seen, last_seen, requests, blocked, geo_status)
			VALUES ({:ip}, {:now}, {:now}, {:req}, {:blk}, {:st})
			ON CONFLICT(ip) DO UPDATE SET
				last_seen = {:now},
				requests = requests + {:req},
				blocked = blocked + {:blk}`).
			Bind(dbx.Params{"ip": ip, "now": now, "req": req, "blk": blk, "st": status}).Execute()
		if err != nil {
			// put the counts back and retry next flush
			c.requests.Add(req)
			c.blocked.Add(blk)
			return true
		}

		flushed++
		return flushed < clientFlushBatch
	})
}

// ---------------------------------------------------------------------
// geolocation worker

// geoLookupFn resolves one IP. Overridable in tests.
type geoResult struct {
	Status      string  `json:"status"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"region"`
	City        string  `json:"city"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
}

func (r *Replicator) lookupGeoIPAPI(ip string) (*geoResult, error) {
	req, err := http.NewRequest(http.MethodGet,
		"http://ip-api.com/json/"+url.PathEscape(ip)+"?fields=status,countryCode,region,city,lat,lon", nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ip-api status %d", resp.StatusCode)
	}
	var out geoResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// geoLoop resolves pending IPs one by one, respecting ip-api.com's free
// rate limit. Exactly one successful lookup is ever made per IP - the
// result (or a permanent failure) is cached in the table.
func (r *Replicator) geoLoop() {
	defer r.wg.Done()

	if r.cfg.DisableIPGeolocation {
		return
	}
	if r.geoLookup == nil {
		r.geoLookup = r.lookupGeoIPAPI
	}

	ticker := time.NewTicker(geoLookupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.geoStep()
		}
	}
}

// geoStep resolves at most ONE pending IP and caches the result.
// Returns true when a lookup was performed.
func (r *Replicator) geoStep() bool {
	db := r.app.NonconcurrentDB()

	var row clientRow
	err := db.NewQuery(`SELECT * FROM _repl_client_ips
		WHERE geo_status = '' ORDER BY last_seen DESC LIMIT 1`).One(&row)
	if err != nil {
		return false // nothing pending
	}

	res, err := r.geoLookup(row.IP)
	if err != nil {
		return true // network hiccup: stays pending, retried next tick
	}

	status := geoFailed
	if res.Status == "success" {
		status = geoOK
	}
	region := ""
	if res.CountryCode != "" && res.Region != "" {
		region = res.CountryCode + "-" + res.Region
	}
	_, _ = db.NewQuery(`UPDATE _repl_client_ips SET
			country = {:c}, region = {:r}, city = {:ci},
			lat = {:la}, lon = {:lo}, geo_status = {:st}
		WHERE ip = {:ip}`).
		Bind(dbx.Params{
			"c": res.CountryCode, "r": region, "ci": res.City,
			"la": res.Lat, "lo": res.Lon, "st": status, "ip": row.IP,
		}).Execute()
	return true
}

// gcClients prunes stale/excess client rows (called from compact).
func gcClients(db dbx.Builder, retention time.Duration) error {
	cutoff := time.Now().UTC().Add(-retention).Format(time.RFC3339)
	if _, err := db.NewQuery(`DELETE FROM _repl_client_ips WHERE last_seen < {:cut}`).
		Bind(dbx.Params{"cut": cutoff}).Execute(); err != nil {
		return err
	}
	_, err := db.NewQuery(fmt.Sprintf(`DELETE FROM _repl_client_ips WHERE ip NOT IN (
		SELECT ip FROM _repl_client_ips ORDER BY last_seen DESC LIMIT %d)`, maxTrackedClients)).Execute()
	return err
}

// ---------------------------------------------------------------------
// dashboard endpoints

type clientsResponse struct {
	Clients          []clientRow `json:"clients"`
	BlockedCountries []string    `json:"blocked_countries"`
	BlockedRegions   []string    `json:"blocked_regions"`
	GeoEnabled       bool        `json:"geo_enabled"`
}

func (r *Replicator) handleClients(e *core.RequestEvent) error {
	var rows []clientRow
	err := r.app.DB().NewQuery(fmt.Sprintf(
		`SELECT * FROM _repl_client_ips ORDER BY last_seen DESC LIMIT %d`, maxTrackedClients)).All(&rows)
	if err != nil {
		return e.InternalServerError("failed to list clients", nil)
	}

	resp := &clientsResponse{
		Clients:    rows,
		GeoEnabled: !r.cfg.DisableIPGeolocation,
	}

	// active deny rules for the map overlay
	fw := r.firewall
	fw.mu.RLock()
	for i := range fw.rules {
		rule := &fw.rules[i]
		if rule.action != fwActionDeny {
			continue
		}
		switch rule.matchType {
		case fwMatchCountry:
			resp.BlockedCountries = append(resp.BlockedCountries, rule.value)
		case fwMatchRegion:
			resp.BlockedRegions = append(resp.BlockedRegions, rule.value)
		}
	}
	fw.mu.RUnlock()

	return e.JSON(http.StatusOK, resp)
}

func (r *Replicator) handleWorldMap(e *core.RequestEvent) error {
	data, err := dashboardFS.ReadFile("dashboard/world.json")
	if err != nil {
		return e.NotFoundError("world map asset missing", nil)
	}
	e.Response.Header().Set("Cache-Control", "max-age=86400")
	return e.Blob(http.StatusOK, "application/json", data)
}
