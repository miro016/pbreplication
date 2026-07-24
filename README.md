# pbreplication

Active-active (multi-master) replication for [PocketBase](https://pocketbase.io).

Add one line to your PocketBase app and every instance becomes a full
read/write cluster node. Writes made on any node appear on all the
others — including file uploads, deletions and schema changes — while
**all PocketBase features keep working**: your Go hooks fire and
realtime (live) subscriptions emit on every node, even for changes that
arrived via replication.

```go
app := pocketbase.New()

pbreplication.MustRegister(app, pbreplication.Config{
    NodeURL:       "http://node2:8090",       // how OTHER nodes reach this one
    SeedURL:       "http://node1:8090",       // any existing member (empty on the very first node)
    ClusterSecret: "at-least-16-characters",  // shared cluster password
})

app.Start()
```

That's the whole cluster setup: **one seed node URL + one shared
password**. Everything else (full member list, health, catching up
after downtime) is discovered and handled automatically.

## Features

- **All nodes are active** — read and write on any node.
- **Conflict resolution: last update wins**, using hybrid logical
  clocks (robust against moderate wall-clock skew, deterministic
  tiebreak).
- **No extra ports** — replication traffic uses PocketBase's regular
  HTTP(S) port under `/api/replication/*`, authenticated with an
  HMAC derived from the cluster secret. Optionally, a dedicated
  intranet-only port for node-to-node traffic (`ReplicationBindAddr`).
- **Autodiscovery** — a new node only knows one seed; the full member
  list gossips to everyone within one sync round.
- **Hooks & realtime preserved** — replicated changes go through the
  normal PocketBase save/delete pipeline, so `OnRecord*` hooks run and
  SSE realtime events fire on every node.
- **Files replicate** — uploads are fetched from peers and stored under
  the same keys; protected file tokens work cluster-wide.
- **Deletes propagate** via tombstones, and survive node downtime.
- **Fast bootstrap: whole-database copy** — a brand-new node downloads
  the seed's entire SQLite database as one consistent snapshot
  (`VACUUM INTO`, chunked + resumable + checksummed) *before*
  PocketBase even opens it, then pulls only the deltas. Orders of
  magnitude faster than row-by-row sync for large databases; falls back
  to the logical sync against older peers automatically.
- **Nodes can join late or rejoin** — anti-entropy replays missed
  operations; too-stale nodes resync with a logical snapshot (resumable
  across restarts) or, with `ResyncStrategy: "restart-copy"`, a full
  database copy that first **rescues local writes the cluster never
  saw** and replays them afterwards.
- **Migrations are safe & cluster-coordinated** — schema changes
  replicate as idempotent operations; the same migration running on
  several nodes converges (content-hash dedup + LWW). On every start a
  clustered node first asks its peers which migrations already ran and
  executes only the rest — so a data-seeding migration can never run
  twice and collide with rows arriving via sync.
- **Cluster dashboard** — `/api/replication/dashboard` shows every
  node, its health, per-peer replication lag, live sync progress (with
  ETA and MB/s during a database copy), the relation-integrity status
  and a filterable event timeline.
- **Observability built in** — a ring buffer of typed replication
  events (`/api/replication/events`, node joins, health flips, sync
  lifecycle, failed ops, firewall blocks…), structured logs
  (`component=pbreplication`), periodic lag summaries, and an exported
  Go API (`Status()`, `SyncStatus()`, `PeerLags()`, `Events()`,
  `OnEvent()`, `RunIntegrityCheck()`…).
- **Relation integrity validated** — replicated relations can never
  fail to apply (they converge as ops arrive); after every bulk sync a
  background pass verifies no dangling references remain and reports
  via logs, events, `/status` and the Go API.
- **Built-in firewall** — allow/deny rules by IP, CIDR range, country
  or region, managed from the dashboard, enforced on all routes with a
  separate scope for the replication endpoints. Rules replicate
  cluster-wide automatically, and country rules work out of the box via
  a bundled GeoIP database (IP geolocation by [DB-IP](https://db-ip.com)).
- **Client world map** — the dashboard plots every unique client IP on
  a world map (geolocated once and cached — locally by default, or via
  ip-api.com when enabled), with blocked clients in red and countries
  under a deny rule shaded.
- **Light on resources** — batched debounced pushes, periodic pulls,
  a single applier goroutine, oplog compaction and full garbage
  collection of all bookkeeping tables.

## How it works?
The core idea: every node keeps a normal PocketBase SQLite database, plus a small operation log. Whenever a record is created, updated, or deleted, that change is written to the log in the same SQLite transaction — so the log and the data can never disagree.

How changes spread: nodes talk to each other over plain HTTP/JSON on PocketBase's own port (or the dedicated replication port), authenticated with an HMAC built from the shared cluster secret. Two mechanisms work together:

Push — after a write, a node sends the new log entries to its peers (batched and debounced, so bursts of writes become few requests).
Pull — on a fixed interval, every node also asks each peer "what do you have that I haven't seen?", exchanging progress vectors. So even if a push was missed (node down, network blip), the gap is repaired within one sync round.
Conflict resolution: if the same record was changed on two nodes at once, last write wins. "Last" is decided by a hybrid logical clock (HLC) — a timestamp combining wall-clock time with a logical counter, so it stays correct even when servers' clocks drift a bit.

Applying changes: incoming operations go through PocketBase's regular save/delete pipeline, which is why your hooks and realtime subscriptions still fire on every node, without causing replication loops.

Joining: a new node only needs one existing member's URL and the secret. It copies the seed's whole database file for a fast start, then stays current via push/pull. The member list gossips automatically.

Tech stack: pure Go, SQLite (via PocketBase), HTTP/JSON, HMAC signatures, hybrid logical clocks — no external broker, no Raft/consensus, no extra infrastructure. It's "eventually consistent": all nodes accept writes and converge to the same state within about one sync round.


## Try it (Docker)

A ready-made 3-node demo cluster:

```bash
cd example
docker compose up --build
```

| Node  | API/Admin UI            |
|-------|-------------------------|
| node1 | http://localhost:8091   |
| node2 | http://localhost:8092   |
| node3 | http://localhost:8093   |

1. Create a superuser on node1:
   `docker compose exec -e PBR_CLUSTER_SECRET=change-me-please-0123456789 node1 pb superuser upsert admin@example.com your-password --dir=/pb_data`
2. Log into http://localhost:8091/_/ and create a collection + records
   (try a file field!).
3. Open http://localhost:8092/_/ — everything is there. Your superuser
   login and auth tokens work on every node.
4. Open http://localhost:8091/api/replication/dashboard to watch the
   cluster. Stop a node (`docker compose stop node3`), write some data,
   start it again — it catches up, deletions included.

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `NodeURL` | `""` | Base URL under which **other** nodes reach this node. Behind a reverse proxy use the public URL. Empty = *pull-only mode* (see below). |
| `SeedURL` | `""` | Any existing member. Empty only for the first node of a new cluster. |
| `ClusterSecret` | — | Shared cluster password, min 16 chars. **Required.** |
| `NodeID` | random | Persistent node id (autogenerated and stored on first start). |
| `SyncInterval` | `10s` | Anti-entropy pull period. |
| `DebounceWindow` | `150ms` | Batching window before pushing local writes. |
| `MaxBatch` | `500` | Max operations per push/pull page. |
| `TombstoneRetention` | `720h` | How long delete markers are kept. Must exceed your longest planned node downtime — nodes offline for longer are resynced with a full snapshot automatically. |
| `CompactionInterval` | `1h` | How often the oplog/bookkeeping garbage collection runs. |
| `ExcludeCollections` | `_mfas, _otps, _authOrigins` | Collections that stay node-local. |
| `ReplicateSuperusers` | `true` | Replicate the `_superusers` collection. |
| `DeferMigrationsUntilSynced` | `true` | Clustered nodes postpone the app's migrations on every start and coordinate with peers, running only migrations no member has applied (see below). |
| `RequestTimeout` | `30s` | Deadline for one node-to-node JSON request. Streaming transfers (files, database chunks) use per-chunk deadlines instead of one global timeout. |
| `MaxBodyBytes` | `16MB` | Max node-to-node request body buffered for HMAC verification. |
| `ApplyBatch` | `200` | Max remote ops applied per SQLite transaction on the receiving node. Batching amortises per-transaction cost when absorbing a peer's bulk writes; FIFO order and per-record LWW checks are unchanged. |
| `ReplicationBindAddr` | `""` | Optional dedicated address for the node-to-node endpoints (e.g. `10.0.0.5:8091` on an intranet interface). When set, they are served **only** there — the public port keeps just the app + operator endpoints. Empty = everything on PocketBase's port (previous behavior). |
| `FullCopyBootstrap` | `true` | New nodes bootstrap by copying the seed's whole database file instead of row-by-row sync (see below). |
| `FullCopyChunkSize` | `8MB` | Chunk size for database snapshot downloads (each chunk retried independently). |
| `FullCopyFallbackAfter` | `10m` | How long a failing full copy is retried before falling back to the logical sync. |
| `SnapshotCacheTTL` | `10m` | How long a prepared database snapshot is reused for additional joiners. |
| `ResyncStrategy` | `"logical"` | How a node that fell behind compaction resyncs: `"logical"` (in-process row sync) or `"restart-copy"` (flag + full DB copy on next restart, rescuing local writes). |
| `MigrationCoordinationTimeout` | `30s` | How long a starting node waits for peers to report executed migrations before running its deferred migrations locally. |
| `IntegrityCheckAfterSync` | `true` | Run a relation-integrity scan (dangling reference check) after bulk syncs. |
| `EventBufferSize` | `512` | Capacity of the in-memory replication event timeline. |
| `DisableUIExtension` | `false` | Turn off the "Replication" tab injected into the admin UI. |
| `GeoIPDBPath` | `""` | Custom MaxMind-format `.mmdb` for country/region firewall rules and client geolocation. A city-level database (GeoLite2-City, DB-IP City Lite) also enables region rules and map coordinates. When empty, the bundled DB-IP Country Lite database is used. |
| `DisableEmbeddedGeoIP` | `false` | Don't load the bundled DB-IP country database (~8 MB RAM). Without any GeoIP database, country/region rules are ignored with a warning. |
| `FirewallExemptSuperusers` | `true` | Superuser-authenticated requests bypass app-scope firewall rules (lock-out guard). |
| `DisableIPGeolocation` | `false` | Turn off the automatic one-time geolocation of client IPs (dashboard map) entirely. |
| `EnableIPAPIGeolocation` | `false` | Geolocate client IPs via the external [ip-api.com](https://ip-api.com) service (adds city + map coordinates) instead of the local GeoIP database. Off by default — no client IP leaves the node unless you opt in. |
| `IPAPIKey` | `""` | Optional ip-api.com paid ("pro") API key. Implies `EnableIPAPIGeolocation` and uses the HTTPS pro endpoint with a higher rate limit. |

The example app maps `PBR_NODE_URL`, `PBR_SEED_URL`,
`PBR_CLUSTER_SECRET`, `PBR_GEOIP_DB`, `PBR_ENABLE_IPAPI` and
`PBR_IPAPI_KEY` env vars to these fields.

### Behind a reverse proxy / NAT

- Set `NodeURL` to the **public** URL (e.g. `https://pb1.example.com`).
  When a node joins, the seed verifies the advertised URL with a
  callback ping before other nodes start dialing it.
- If a node cannot be reached from outside at all, leave `NodeURL`
  empty: it runs in **pull-only mode** — it initiates every exchange
  itself (pushes its writes out, pulls everything else in), so no peer
  ever needs to connect to it. It still appears on the dashboard.
- Configure PocketBase's *trusted proxy headers* (Admin UI → Settings →
  Application) so the firewall sees real client IPs.

### Dedicated replication port (public app / intranet cluster)

When the app port is exposed to the internet but the cluster members
share a private network, set `ReplicationBindAddr` to an intranet
interface (e.g. `10.0.0.5:8091`). The node-to-node endpoints
(join/ping/ops/pull/file/snapshot/migrations) then bind **only** there:
the public port answers 404 for them, so replication traffic is
unreachable from outside by construction (on top of the HMAC auth and
the `replication`-scope firewall, which both keep working on the
dedicated listener).

- Set `NodeURL` to the address peers reach the listener on —
  `http://10.0.0.5:8091` — since that is where this node now serves
  replication. The operator endpoints (`/api/replication/dashboard`,
  `/status`, …) stay on the app port.
- The listener speaks plain HTTP, which is the normal choice inside a
  private network; put a TLS proxy in front of it if the link crosses
  untrusted infrastructure.
- When switching an existing node over, update its `NodeURL` at the
  same time — peers pick the new URL up automatically on the next
  authenticated exchange.
- Not set? Everything behaves exactly as before, on PocketBase's own
  port.

## Dashboard

The easiest way in: log into the admin UI (`/_/`) — a **Replication**
tab appears in the sidebar and opens the dashboard right inside the
admin UI, authenticated with your existing login (no extra token). This
tab uses PocketBase's experimental UI-extension API; if a future PB
version changes the admin internals it degrades to a simple link, and
you can turn it off with `DisableUIExtension: true`.

The dashboard is also available standalone at
`GET /api/replication/dashboard` on any node (it picks up your admin UI
login from the browser automatically). Two tabs:

- **Nodes**: every member, its URL (or pull-only), health, applied
  sequence and **replication lag** (how many of this node's operations
  the peer hasn't acknowledged yet), oplog size, pending/failed
  operations, and the last relation-integrity result. During a bulk
  sync (initial sync, resync, database copy, blob backfill, integrity
  check) a **live progress banner** shows the phase, percent, rows or
  MB transferred and the ETA.
- **Events**: the replication event timeline — node joins/removals,
  peer health transitions, snapshot/copy start+finish, failed
  operations, migration runs, firewall blocks and integrity reports,
  filterable by type (backed by `GET /api/replication/events`).
- **Firewall**: manage allow/deny rules, see per-scope mode
  (blacklist/whitelist), GeoIP status and the blocked-request counter.
- **Map**: a world map of every unique client IP this node has seen —
  blue dots for allowed clients, red for clients with blocked requests,
  countries under an active deny rule shaded red, and blocked regions
  listed. Below the map, a searchable **client IP list**: click any IP
  to see its geo record and its top request paths with per-path
  request/blocked counts — handy for spotting automated or suspicious
  traffic. Each new public IP is geolocated **once** and cached
  forever; private/loopback addresses are counted but not located. By
  default the lookup is **local**, against the same GeoIP database the
  firewall uses (the bundled country database resolves the country;
  a city-level `GeoIPDBPath` also yields city + map dots) — no client
  IP ever leaves the node. Set `EnableIPAPIGeolocation: true` (or an
  `IPAPIKey`) to use the external [ip-api.com](https://ip-api.com)
  service instead for city-level map dots without a city database, or
  `DisableIPGeolocation: true` to not geolocate at all. Client IPs and
  paths are tracked per node, kept for `TombstoneRetention` (capped at
  10k IPs and 200 paths per IP), and correct client addresses behind a
  proxy require PocketBase's trusted-proxy settings.

Firewall country/region rules are picked from a multi-select in the
dashboard (search by name; one rule is created per selection), and the
IP/CIDR inputs show format-specific placeholders.

Data endpoints (`/api/replication/status`, `/api/replication/events`,
`/api/replication/integrity`, `/api/replication/firewall/summary`)
require superuser auth.

## Firewall

Rules live in the (superuser-only) `pbr_firewall_rules` collection, so
they replicate to every node and can also be managed via the normal
records API. Each rule:

- `action`: `allow` | `deny`
- `scope`: `app` (everything) | `replication` (only `/api/replication/*`)
- `match_type`: `ip` | `cidr` | `country` | `region`
- `value`: `203.0.113.7`, `10.0.0.0/8`, `US`, `US-CA`, …
- `active`: on/off

Semantics: an explicit **deny** match always blocks. As soon as a scope
has at least one active **allow** rule it flips to *whitelist mode* —
everything not explicitly allowed is blocked. Example: one rule
`allow / replication / cidr / 10.0.0.0/8` pins the replication
endpoints to your private network.

Safety rails: loopback is never blocked, superusers bypass app-scope
rules by default, and the replication endpoints always require the
cluster HMAC regardless of firewall rules (the firewall is
defense-in-depth there, not the only gate).

**Country rules work out of the box**: the package bundles the free
[DB-IP Country Lite](https://db-ip.com) database (CC BY 4.0 — "IP
Geolocation by DB-IP"), refreshed automatically every month by CI so
each release ships a current snapshot. **Region rules** need
subdivision data that country-level databases don't carry: point
`GeoIPDBPath` at a city-level `.mmdb` (e.g. MaxMind's free
GeoLite2-City — its license prevents bundling — or DB-IP City Lite)
and restart. A custom `GeoIPDBPath` always takes precedence over the
bundled database, and `DisableEmbeddedGeoIP: true` skips loading it.

A country/region rule that **cannot** match — no GeoIP database at
all, or a region rule with a country-only database — is ignored
("inert") rather than compiled, so a broken allow rule can't flip a
scope into whitelist mode and lock everyone out. Every ignored rule is
reported: a warning in the logs, in `GET
/api/replication/firewall/summary` (`warnings`), and prominently in
the dashboard's Firewall tab.

## How it works (short version)

Every local write is recorded — inside the same SQLite transaction — as
an entry in an operation log with a hybrid-logical-clock timestamp.
Nodes push fresh entries to their peers (debounced/batched) and
additionally pull from every peer on a fixed interval, exchanging
per-source progress vectors, so any missed operation is repaired within
one sync round; nodes re-serve relayed entries, so updates spread even
when some nodes can't reach each other directly. An incoming operation
is applied only if it is newer (LWW) than what the record already has,
and it is applied through PocketBase's regular save/delete pipeline
with a context marker — that is why your hooks and realtime events fire
without creating replication loops. Old log entries are compacted away
(the newest operation per record always survives; delete markers are
kept for `TombstoneRetention`), and nodes that fall behind the
compaction horizon are automatically resynced from a full snapshot.

## Fast bootstrap: the full database copy

With `FullCopyBootstrap` (default on), a brand-new node joining a
cluster doesn't crawl the seed row by row. The startup sequence is:

1. **Before PocketBase opens its database**, the node asks the seed for
   a database snapshot (`POST /api/replication/snapshot/db`). The seed
   produces a consistent point-in-time copy with SQLite's `VACUUM INTO`
   (WAL-safe, no partial state) and advertises its size, SHA-256, and
   the replication vector captured *before* the vacuum. Snapshots are
   cached for `SnapshotCacheTTL`, so ten joiners cost one vacuum.
2. The file downloads in `FullCopyChunkSize` chunks — each chunk has
   its own deadline and retry, transfers are gzip-compressed, and an
   interrupted download **resumes at the exact byte offset** even
   across process restarts. The finished file is verified against the
   manifest checksum.
3. The copy is rewritten as *this* node: fresh node identity, adopted
   vector, cleared node-local telemetry, emptied `ExcludeCollections`.
4. The file is atomically installed as `data.db`, PocketBase opens it,
   and the serve-time migration runner executes **only the migration
   files missing from the copied `_migrations` table** — the cluster's
   migration history travels inside the copy.
5. The node joins the cluster and the first anti-entropy pull fetches
   just the operations written since the vacuum. Uploaded files are
   backfilled in the background (`blob backfill`), and a
   relation-integrity check runs once everything settles.

If the seed runs an older pbreplication without snapshot support (404),
or the copy keeps failing for `FullCopyFallbackAfter`, the node falls
back to the classic logical row-by-row bootstrap automatically.

### Long-offline nodes (`ResyncStrategy`)

A node offline longer than `TombstoneRetention` can't replay the oplog
(deletes were compacted away) and must resync:

- `"logical"` (default): an in-process row-by-row snapshot sync with
  reconcile — no restart needed. Progress is resumable across restarts
  and applies in batched transactions.
- `"restart-copy"`: the node flags itself (`resync_pending`), logs and
  emits a `RESTART THIS NODE` event, and on the next start replaces its
  database with a full copy. **Local writes the cluster never received
  are rescued first** (to `.pbreplication/rescue.json`) and re-applied
  after startup through the normal conflict resolution with their
  original timestamps — a newer cluster write still wins, everything
  else replicates out. Best for very large databases where a row-by-row
  resync would take hours.

The auxiliary database (`auxiliary.db`, PocketBase's `_logs`) is
node-local and never copied.

## Go API

Beyond the HTTP endpoints, the `*Replicator` handle returned by
`Register`/`MustRegister` exposes the cluster to your Go code:

```go
r := pbreplication.MustRegister(app, cfg)

r.NodeID()      // this node's persistent id
r.Ready()       // finished bootstrapping?
r.Members()     // []MemberInfo: every member incl. health
r.PeerURLs()    // healthy peer id -> URL
r.LeaderID()    // deterministic leader (lowest healthy id)
r.IsLeader()    // gate singleton work (cron jobs, ...)

r.Status()      // ClusterStatus: everything /status returns, typed
r.SyncStatus()  // live bulk-sync phase/progress/ETA
r.Counters()    // applied/failed/blocked, oplog size, backlogs
r.PeerLags()    // per-peer: how many of our ops they haven't acked
r.LastError()   // most recent replication error

r.Events(100)                  // newest events from the ring buffer
unsub := r.OnEvent(func(ev pbreplication.Event) { ... }) // subscribe
r.RunIntegrityCheck(ctx)       // on-demand dangling-relation scan
r.LastIntegrityReport()        // result of the last scan
```

## Limitations & operational notes

- **Eventual consistency.** Replication is asynchronous; a read on
  another node may briefly return stale data. Conflicts are resolved
  per **whole record** — concurrent updates to the same record keep one
  side.
- **Unique constraints**: two nodes inserting the same unique value at
  the same time will conflict; the losing operation is reported on the
  dashboard (`failed ops`) and needs a manual fix.
- **Clocks**: HLC tolerates skew, but keep NTP running — LWW is only as
  fair as your clocks.
- **Auth**: auth records (including password hashes and token keys) and
  the collections' token secrets replicate, so a JWT issued by one node
  is valid on all. Password changes/token invalidations propagate with
  replication lag. `_mfas`/`_otps`/`_authOrigins` stay node-local.
- **Migrations**: run the same binary/PB version on every node (natural
  with one Docker image). Create collections via migrations or on a
  single node — avoid creating a same-named collection independently on
  two nodes at once. See "Migrations & seeding on joining nodes" below
  for what happens on a fresh node.
- **Files**: local storage only (each node keeps a full copy). If you
  use S3 storage, point all nodes at the same bucket and file
  replication becomes a no-op.
- **Throughput**: SQLite has a single writer per node; replication adds
  one small insert per write plus background batches. For very
  write-heavy workloads, measure.
- **Backups/restore**: restore ONE node from backup, wipe the data dirs
  of the others and let them re-bootstrap from it (fresh `SeedURL`).
- **Settings** (SMTP, app name, …) are not replicated — configure per
  node (they're usually env-driven anyway).
- The admin-UI "Replication" tab uses PocketBase's experimental UI
  extension API and may break on PB upgrades (it degrades to a plain
  link); the standalone dashboard always works. Opt out with
  `DisableUIExtension: true`.
- **Security**: the cluster secret grants full read/write to the whole
  database. Use long random secrets, HTTPS or a private network between
  nodes, and consider a `replication`-scope firewall whitelist.

## Migrations & seeding on clustered nodes

App migrations on a clustered node (a `SeedURL` is configured, or peers
are already known) are **coordinated with the cluster on every start**,
not just the first one:

1. At startup the app's migrations are held back, so PocketBase's
   serve-time runner applies only its own system migrations.
2. A brand-new node gets the effects of every past migration with the
   bootstrap (full database copy or snapshot sync) and imports the
   cluster's `_migrations` history without executing anything.
3. The node then asks **every reachable peer** which migration files it
   executed (`GET /api/replication/migrations`) and marks the union as
   applied — covering migrations some other node ran while this one was
   offline.
4. Only migrations **no member has run** execute locally; their writes
   replicate out normally.

This closes the classic duplicate-seed hazard: a migration that inserts
records with generated ids can no longer run on two nodes and produce
duplicated rows — whichever node runs it first wins, everyone else
imports it as done.

Fallbacks and edge cases:

- If **no peer answers** within `MigrationCoordinationTimeout`, the
  node runs its deferred migrations locally — it never stays
  schema-less waiting for an unreachable cluster.
- A standalone node (no seed, no known peers) never defers — plain
  PocketBase behavior.
- Two nodes restarted **simultaneously** with the same brand-new
  migration: non-leader nodes wait one `SyncInterval` and re-check
  before running, which narrows but cannot fully eliminate the race
  (there is no distributed lock). Write seeding migrations
  idempotently (fixed ids, or guard with an existence check).
- Set `DeferMigrationsUntilSynced: false` to restore plain
  migrate-at-startup behavior everywhere.

### Startup & migration logs

Key lifecycle milestones are written to **both** the PocketBase logger
(persisted in the `_logs` table and visible in the admin UI) **and** the
process stdout, so you can follow a joining node live in the console:

```
2026/07/09 09:12:03 [pbreplication] instance connected to cluster node=abc123 seed=http://node1:8090 members=2
2026/07/09 09:12:03 [pbreplication] starting initial data migration (full snapshot sync from seed) node=abc123 seed=http://node1:8090
2026/07/09 09:12:03 [pbreplication] estimating full sync duration rows_to_sync=1500000
2026/07/09 09:12:18 [pbreplication] full sync progress rows=210000 total=1500000 percent=14 eta=1m47s ready_by=2026-07-09 09:14:05
2026/07/09 09:13:59 [pbreplication] migrated "posts": 1280000 rows
2026/07/09 09:14:04 [pbreplication] migrated "users": 220000 rows
2026/07/09 09:14:04 [pbreplication] initial data migration complete collections=2 rows=1500000 took=2m1s
2026/07/09 09:14:04 [pbreplication] initial bootstrap complete node=abc123 seed=http://node1:8090
```

Each collection shows a live, in-place progress counter while its rows
are streaming in, then settles into a final per-collection total. The
completion line reports how many collections and rows were migrated and
how long the sync took.

**Estimated completion time (ETA).** For large databases the seed
reports its per-collection row counts, so a joining node knows the total
up front and estimates when the full sync will finish. The live console
line shows an `ETA` for the whole sync, and a throttled `full sync
progress` line — carrying the percent complete, the estimated time
remaining (`eta`), and the projected wall-clock finish time
(`ready_by`) — is persisted to the `_logs` table roughly every 15
seconds. (A seed running an older pbreplication version doesn't report
counts; the sync still runs, just without an ETA.)

A snapshot resync (triggered when a peer has compacted past this node's
cursor) logs the same way.

Ongoing anti-entropy pulls are logged too, but only when they actually
carry new operations — an idle cluster stays quiet:

```
2026/07/09 09:20:14 [pbreplication] pulled 37 ops from node2
```

Large catch-ups (a node returning after downtime) show the same live,
in-place progress counter while paging through the backlog, then settle
into the final `pulled N ops from <peer>` line. These pull events are
written to both stdout and the `_logs` table.

Caveats:

- On a fresh joining node `./app migrate up` reports nothing to apply
  until the first successful sync — the deferral also holds for the
  CLI.
- Migrations whose effects don't replicate (seeding records into
  `ExcludeCollections`, raw node-local SQL) are marked applied but
  their node-local effects won't exist on joiners. Keep node-local
  seeding out of app migrations, or disable the option.
- Mixed-version rollout: a seed running a pbreplication version older
  than this feature can't report its migration history; the joiner then
  assumes *all* of its migrations were already applied cluster-wide (a
  warning is logged). Upgrade the seed node before shipping new
  migrations.

## Troubleshooting

**The "Replication" tab doesn't show up in the admin UI**

1. Make sure the running image/binary is current. The demo:
   `git pull && docker compose build --no-cache && docker compose up -d`.
   On startup each node logs `admin UI extension registered` — if that
   line is missing, the binary is old or `DisableUIExtension` is set.
2. Server-side check:
   `curl -s http://localhost:8091/_/extensions.js | grep -c Replication`
   — a number ≥ 1 means the server is fine and it's your **browser
   cache**: PocketBase serves `/_/extensions.js` with a 14-day cache
   header, so a browser that visited the admin UI before the extension
   existed keeps using the old empty file. Hard-refresh the admin page
   once (`Ctrl+Shift+R` / `Cmd+Shift+R`). Current versions of this
   package override that header with `no-cache`, so this is a one-time
   fix — future extension updates are picked up automatically.
3. **Behind a CDN (e.g. Cloudflare)**: the edge may cache
   `/_/extensions.js` and rewrite its cache headers (Cloudflare's
   *Browser Cache TTL* setting overrides low origin max-age values), so
   tab updates can lag even with the `no-cache` fix above. Set Browser
   Cache TTL to "Respect Existing Headers" or add a cache rule that
   bypasses caching for `/_/extensions.js`, then purge the edge cache
   once.

**A node joins fine but goes offline seconds later and replication
stops** — the classic cause: a node advertises a `PBR_NODE_URL` that
the *other* nodes cannot reach (e.g. a docker-internal name like
`http://node1:8090` while the peer sits on another host and joined
through a public domain). The join works because it uses your
configured seed URL, but the periodic sync switches to the advertised
URLs from the member list and starts failing. Fixes:

- Set every node's `PBR_NODE_URL` to a URL that is reachable **from the
  other nodes** (public domain, VPN/private IP, ...). This is the
  proper fix for multi-host clusters.
- The dashboard's node table shows the exact failing URL and error
  (⚠ under the URL), and each node logs a warning at join time when its
  advertised URL can't be called back.
- As a safety net, a joining node that can't reach the seed's
  advertised URL automatically keeps using the configured seed URL for
  that peer (shown as "override" in the dashboard).

**Both nodes show the same node id, and the cluster table only lists one
node ("this node") on each instance** — the second node was started on a
*copy* of the first node's `pb_data` directory (or `data.db`), so it
inherited the first node's persisted identity. Two nodes then run under
one id, every join looks like a self-announcement, and nothing
replicates. Current versions detect this automatically: on startup the
node probes its seed and, when the seed answers with the node's own id,
regenerates a fresh identity in place (local data is kept; writes the
original doesn't know about are re-emitted under the new id). If the
duplicate is only discovered later (e.g. both twins started at the same
time), the node logs `ANOTHER CLUSTER NODE ALREADY USES THIS NODE'S ID`
and heals itself on its next restart. To avoid the situation entirely:
never clone `pb_data` to provision a node — start the new node with an
**empty** data directory and let the full-copy bootstrap transfer the
data (it assigns a fresh identity as part of the copy).

**Nodes don't see each other** — check that all nodes share the exact
same `PBR_CLUSTER_SECRET`, that the seed URL is reachable *from inside*
the node's network namespace (in compose, use service names like
`http://node1:8090`, not `localhost`), and look at
`/api/replication/status` / the dashboard for the per-node ⚠ errors and
`last_error`. If node-to-node traffic passes through a CDN/WAF
(Cloudflare etc.), make sure bot protection does not challenge the
server-to-server calls — add a WAF skip rule for `/api/replication/*`,
or better, point `PBR_NODE_URL` at a direct origin address so cluster
traffic bypasses the CDN entirely.

## Testing

```bash
go test ./...
```

The suite covers the HLC, LWW gating, capture→apply round-trips
(including autodate preservation and hook firing), schema-op
idempotence, HMAC auth (incl. body-size caps), transport retries and
resumable streams, the event ring buffer and health transitions, peer
lag, batched snapshot applies with persisted resume cursors, the paged
reconcile, the full database copy end-to-end (vacuum, chunked resume,
sanitize, offline-write rescue/replay, old-peer fallback), migration
coordination, relation-integrity scans, compaction/garbage collection
and the firewall matcher. The `example/` compose file doubles as an
end-to-end test bed.

## Releases & versioning

Versioning is automated with
[release-please](https://github.com/googleapis/release-please). Commits to
`main` use [Conventional Commits](https://www.conventionalcommits.org)
(`feat:`, `fix:`, `docs:`, `chore:`, …). release-please keeps an open
"release PR" that bumps the version and updates `CHANGELOG.md`; merging it
tags `vX.Y.Z` and publishes a GitHub release. Downstream projects then:

```bash
go get github.com/miro016/pbreplication@vX.Y.Z
```

No manual tagging or registry upload is needed — Go fetches modules
straight from the tagged git commit.

## License

MIT
