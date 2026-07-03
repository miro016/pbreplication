// Example PocketBase app with cluster replication enabled.
//
// Configuration via environment variables:
//
//	PBR_NODE_URL       - this node's URL as reachable by OTHER nodes
//	PBR_SEED_URL       - URL of any existing cluster member (empty on the first node)
//	PBR_CLUSTER_SECRET - shared cluster password (min 16 chars)
//	PBR_GEOIP_DB       - optional path to a MaxMind .mmdb for country firewall rules
//	PBR_IPAPI_KEY      - optional ip-api.com paid API key (dashboard map geolocation)
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
		NodeURL:       os.Getenv("PBR_NODE_URL"),
		SeedURL:       os.Getenv("PBR_SEED_URL"),
		ClusterSecret: os.Getenv("PBR_CLUSTER_SECRET"),
		GeoIPDBPath:   os.Getenv("PBR_GEOIP_DB"),
		IPAPIKey:      os.Getenv("PBR_IPAPI_KEY"),
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
