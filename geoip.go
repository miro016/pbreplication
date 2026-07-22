package pbreplication

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"fmt"
	"io"
	"strings"

	"github.com/oschwald/maxminddb-golang"
)

// The bundled GeoIP database: the free DB-IP Country Lite database
// (https://db-ip.com), licensed under Creative Commons Attribution 4.0
// ("IP Geolocation by DB-IP"). Unlike MaxMind's GeoLite2 it may be
// redistributed, so country firewall rules and client-map geolocation
// work out of the box with zero configuration and zero external calls.
//
// The file is refreshed automatically by .github/workflows/update-geoip.yml
// (DB-IP publishes a new edition monthly), so every release ships a
// current snapshot. A user-supplied database via Config.GeoIPDBPath
// always takes precedence, and Config.DisableEmbeddedGeoIP opts out of
// loading the bundled copy entirely.
//
//go:embed geoip/dbip-country-lite.mmdb.gz
var embeddedGeoDB []byte

// openEmbeddedGeoDB decompresses the bundled database into memory
// (~8 MB) and opens a reader over it.
func openEmbeddedGeoDB() (*maxminddb.Reader, error) {
	zr, err := gzip.NewReader(bytes.NewReader(embeddedGeoDB))
	if err != nil {
		return nil, fmt.Errorf("embedded geoip db: %w", err)
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("embedded geoip db: %w", err)
	}
	return maxminddb.FromBytes(raw)
}

// geoHasRegions reports whether a database carries subdivision (region)
// data. Country-level editions (e.g. "DBIP-Country-Lite",
// "GeoLite2-Country") don't; region rules need a city-level database.
func geoHasRegions(geo *maxminddb.Reader) bool {
	return !strings.Contains(strings.ToLower(geo.Metadata.DatabaseType), "country")
}
