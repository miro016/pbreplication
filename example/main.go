// Example PocketBase app with cluster replication enabled.
//
// Configuration via environment variables:
//
//	PBR_NODE_URL        - this node's URL as reachable by OTHER nodes
//	PBR_SEED_URL        - URL of any existing cluster member (empty on the first node)
//	PBR_CLUSTER_SECRET  - shared cluster password (min 16 chars)
//	PBR_REPL_ADDR       - optional dedicated bind address for the node-to-node
//	                      replication endpoints (e.g. "10.0.0.5:8091" on an
//	                      intranet interface); empty = PocketBase's own port
//	PBR_GEOIP_DB        - optional custom MaxMind-format .mmdb (city-level enables
//	                      region firewall rules); the bundled DB-IP country
//	                      database is used when unset
//	PBR_ENABLE_IPAPI    - set to any value to geolocate client IPs via ip-api.com
//	                      (city+coordinates on the map) instead of the local db
//	PBR_IPAPI_KEY       - optional ip-api.com paid API key (implies PBR_ENABLE_IPAPI)
//	PBR_RESYNC_STRATEGY - optional: "logical" (default) or "restart-copy"
//	                      (full DB copy on restart for nodes that fell
//	                      behind compaction; local writes are rescued)
package main

import (
	"log"
	"os"

	"github.com/miro016/pbreplication"
	"github.com/pocketbase/pocketbase"
)

func main() {
	app := pocketbase.New()

	pbreplication.MustRegister(app, pbreplication.Config{
		NodeURL:                os.Getenv("PBR_NODE_URL"),
		SeedURL:                os.Getenv("PBR_SEED_URL"),
		ClusterSecret:          os.Getenv("PBR_CLUSTER_SECRET"),
		ReplicationBindAddr:    os.Getenv("PBR_REPL_ADDR"),
		GeoIPDBPath:            os.Getenv("PBR_GEOIP_DB"),
		EnableIPAPIGeolocation: os.Getenv("PBR_ENABLE_IPAPI") != "",
		IPAPIKey:               os.Getenv("PBR_IPAPI_KEY"),
		ResyncStrategy:         os.Getenv("PBR_RESYNC_STRATEGY"),
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
