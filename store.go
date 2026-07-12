package pbreplication

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// Op type constants.
const (
	opUpsert    = "upsert"
	opDelete    = "delete"
	opColUpsert = "col_upsert"
	opColDelete = "col_delete"
)

// State keys in _repl_state.
const (
	stateNodeID        = "node_id"
	stateLocalSeq      = "local_seq"
	stateBootstrapDone = "bootstrap_done"
	stateHLC           = "hlc"
	stateVectorPrefix  = "vector." // + node id

	// stateSnapshotResume holds the JSON progress of an interrupted
	// logical snapshot sync so a restart resumes instead of re-paging
	// every collection from scratch.
	stateSnapshotResume = "snapshot_resume"

	// stateResyncPending, when set, makes the next process start replace
	// the local database with a full copy from a peer (rescuing
	// unacknowledged local ops first).
	stateResyncPending = "resync_pending"

	// stateBlobBackfillPending, when set, makes sync rounds scan file
	// fields for blobs missing from local storage (a full DB copy
	// transfers no files) until a pass completes cleanly.
	stateBlobBackfillPending = "blob_backfill_pending"
)

// collectionsColID is the pseudo collection id used in _repl_versions
// rows that track collection (schema) LWW state.
const collectionsColID = "_collections"

// op is both the oplog row and the wire representation of a single
// replicated operation.
type op struct {
	SrcNode  string              `json:"src_node"`
	SrcSeq   int64               `json:"src_seq"`
	HLC      string              `json:"hlc"`
	Type     string              `json:"op_type"`
	ColID    string              `json:"col_id"`
	ColName  string              `json:"col_name"`
	RecordID string              `json:"record_id"`
	Payload  json.RawMessage     `json:"payload,omitempty"`
	Files    map[string][]string `json:"files,omitempty"`
}

type member struct {
	NodeID    string `json:"node_id" db:"node_id"`
	URL       string `json:"url" db:"url"`
	Reachable bool   `json:"reachable" db:"reachable"`
	JoinedAt  string `json:"joined_at" db:"joined_at"`
	LastSeen  string `json:"last_seen" db:"last_seen"`
	Removed   bool   `json:"removed" db:"removed"`
}

type versionRow struct {
	ColID    string `db:"col_id"`
	RecordID string `db:"record_id"`
	HLC      string `db:"hlc"`
	SrcNode  string `db:"src_node"`
	Deleted  bool   `db:"deleted"`
	Updated  string `db:"updated"`
}

type oplogRow struct {
	RowID    int64  `db:"rowid"`
	SrcNode  string `db:"src_node"`
	SrcSeq   int64  `db:"src_seq"`
	HLC      string `db:"hlc"`
	OpType   string `db:"op_type"`
	ColID    string `db:"col_id"`
	ColName  string `db:"col_name"`
	RecordID string `db:"record_id"`
	Payload  string `db:"payload"`
	Files    string `db:"files"`
	Created  string `db:"created"`
}

func (r *oplogRow) toOp() *op {
	o := &op{
		SrcNode:  r.SrcNode,
		SrcSeq:   r.SrcSeq,
		HLC:      r.HLC,
		Type:     r.OpType,
		ColID:    r.ColID,
		ColName:  r.ColName,
		RecordID: r.RecordID,
	}
	if r.Payload != "" {
		o.Payload = json.RawMessage(r.Payload)
	}
	if r.Files != "" {
		_ = json.Unmarshal([]byte(r.Files), &o.Files)
	}
	return o
}

func nowStr() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// createTables creates all replication bookkeeping tables. The "_"
// prefix keeps them invisible to the PocketBase collections layer.
func createTables(app core.App) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS _repl_state (
			key   TEXT PRIMARY KEY NOT NULL,
			value TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS _repl_oplog (
			src_node  TEXT    NOT NULL,
			src_seq   INTEGER NOT NULL,
			hlc       TEXT    NOT NULL,
			op_type   TEXT    NOT NULL,
			col_id    TEXT    NOT NULL,
			col_name  TEXT    NOT NULL,
			record_id TEXT    NOT NULL DEFAULT '',
			payload   TEXT    NOT NULL DEFAULT '',
			files     TEXT    NOT NULL DEFAULT '',
			created   TEXT    NOT NULL,
			PRIMARY KEY (src_node, src_seq)
		)`,
		`CREATE INDEX IF NOT EXISTS _repl_oplog_rec_idx ON _repl_oplog (col_id, record_id, hlc)`,
		`CREATE INDEX IF NOT EXISTS _repl_oplog_created_idx ON _repl_oplog (created)`,
		`CREATE TABLE IF NOT EXISTS _repl_versions (
			col_id    TEXT NOT NULL,
			record_id TEXT NOT NULL,
			hlc       TEXT NOT NULL,
			src_node  TEXT NOT NULL,
			deleted   INTEGER NOT NULL DEFAULT 0,
			updated   TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (col_id, record_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _repl_members (
			node_id   TEXT PRIMARY KEY NOT NULL,
			url       TEXT NOT NULL DEFAULT '',
			reachable INTEGER NOT NULL DEFAULT 0,
			joined_at TEXT NOT NULL DEFAULT '',
			last_seen TEXT NOT NULL DEFAULT '',
			removed   INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS _repl_compaction (
			src_node TEXT PRIMARY KEY NOT NULL,
			min_seq  INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS _repl_client_ips (
			ip         TEXT PRIMARY KEY NOT NULL,
			first_seen TEXT NOT NULL DEFAULT '',
			last_seen  TEXT NOT NULL DEFAULT '',
			requests   INTEGER NOT NULL DEFAULT 0,
			blocked    INTEGER NOT NULL DEFAULT 0,
			country    TEXT NOT NULL DEFAULT '',
			region     TEXT NOT NULL DEFAULT '',
			city       TEXT NOT NULL DEFAULT '',
			lat        REAL NOT NULL DEFAULT 0,
			lon        REAL NOT NULL DEFAULT 0,
			geo_status TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS _repl_client_ips_seen_idx ON _repl_client_ips (last_seen)`,
		`CREATE TABLE IF NOT EXISTS _repl_client_paths (
			ip        TEXT NOT NULL,
			method    TEXT NOT NULL DEFAULT '',
			path      TEXT NOT NULL,
			count     INTEGER NOT NULL DEFAULT 0,
			blocked   INTEGER NOT NULL DEFAULT 0,
			last_seen TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (ip, method, path)
		)`,
		`CREATE INDEX IF NOT EXISTS _repl_client_paths_ip_idx ON _repl_client_paths (ip)`,
		`CREATE TABLE IF NOT EXISTS _repl_sync_seen (
			run_id TEXT NOT NULL,
			col_id TEXT NOT NULL,
			id     TEXT NOT NULL,
			PRIMARY KEY (run_id, col_id, id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := app.NonconcurrentDB().NewQuery(stmt).Execute(); err != nil {
			return fmt.Errorf("pbreplication: create tables: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// _repl_state helpers (db is passed explicitly so calls can join an
// ongoing transaction via txApp.NonconcurrentDB()).

func getState(db dbx.Builder, key string) (string, error) {
	var v string
	err := db.NewQuery(`SELECT value FROM _repl_state WHERE key = {:key}`).
		Bind(dbx.Params{"key": key}).Row(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func setState(db dbx.Builder, key, value string) error {
	_, err := db.NewQuery(`INSERT INTO _repl_state (key, value) VALUES ({:key}, {:value})
		ON CONFLICT(key) DO UPDATE SET value = {:value}`).
		Bind(dbx.Params{"key": key, "value": value}).Execute()
	return err
}

// incrLocalSeq atomically increments and returns the local op sequence.
// Must be called with the same builder (transaction) as the data write.
func incrLocalSeq(db dbx.Builder) (int64, error) {
	_, err := db.NewQuery(`INSERT INTO _repl_state (key, value) VALUES ({:key}, '1')
		ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)`).
		Bind(dbx.Params{"key": stateLocalSeq}).Execute()
	if err != nil {
		return 0, err
	}
	v, err := getState(db, stateLocalSeq)
	if err != nil {
		return 0, err
	}
	var seq int64
	if _, err := fmt.Sscanf(v, "%d", &seq); err != nil {
		return 0, fmt.Errorf("invalid local_seq %q: %w", v, err)
	}
	return seq, nil
}

// ---------------------------------------------------------------------
// oplog

func insertOp(db dbx.Builder, o *op) error {
	files := ""
	if len(o.Files) > 0 {
		b, _ := json.Marshal(o.Files)
		files = string(b)
	}
	_, err := db.NewQuery(`INSERT OR IGNORE INTO _repl_oplog
		(src_node, src_seq, hlc, op_type, col_id, col_name, record_id, payload, files, created)
		VALUES ({:sn}, {:ss}, {:hlc}, {:t}, {:cid}, {:cn}, {:rid}, {:p}, {:f}, {:c})`).
		Bind(dbx.Params{
			"sn": o.SrcNode, "ss": o.SrcSeq, "hlc": o.HLC, "t": o.Type,
			"cid": o.ColID, "cn": o.ColName, "rid": o.RecordID,
			"p": string(o.Payload), "f": files, "c": nowStr(),
		}).Execute()
	return err
}

// opsAfterRowID returns ops with rowid > cursor, ordered by rowid.
// Used by the pusher (per-peer rowid cursors).
func opsAfterRowID(db dbx.Builder, cursor int64, limit int) ([]*op, int64, error) {
	var rows []oplogRow
	err := db.NewQuery(`SELECT rowid, * FROM _repl_oplog WHERE rowid > {:c} ORDER BY rowid LIMIT {:l}`).
		Bind(dbx.Params{"c": cursor, "l": limit}).All(&rows)
	if err != nil {
		return nil, cursor, err
	}
	ops := make([]*op, 0, len(rows))
	last := cursor
	for i := range rows {
		ops = append(ops, rows[i].toOp())
		last = rows[i].RowID
	}
	return ops, last, nil
}

// maxRowID returns the current max oplog rowid (0 when empty).
func maxRowID(db dbx.Builder) (int64, error) {
	var v sql.NullInt64
	err := db.NewQuery(`SELECT MAX(rowid) FROM _repl_oplog`).Row(&v)
	if err != nil {
		return 0, err
	}
	return v.Int64, nil
}

// opsAfterVector returns up to limit ops that the requester (described
// by its per-source contiguous vector) doesn't have yet, plus whether a
// snapshot is required because compaction dropped needed ops.
func opsAfterVector(db dbx.Builder, vector map[string]int64, limit int) (ops []*op, snapshotRequired bool, err error) {
	// all source nodes present in our oplog or compaction table
	var sources []string
	err = db.NewQuery(`SELECT src_node FROM (
		SELECT DISTINCT src_node FROM _repl_oplog
		UNION SELECT src_node FROM _repl_compaction) ORDER BY src_node`).Column(&sources)
	if err != nil {
		return nil, false, err
	}

	for _, src := range sources {
		if limit <= 0 {
			break
		}
		after := vector[src]

		// If compaction dropped tombstone ops the requester still needs,
		// a plain replay would silently skip deletes -> demand a snapshot.
		var horizon sql.NullInt64
		err = db.NewQuery(`SELECT min_seq FROM _repl_compaction WHERE src_node = {:s}`).
			Bind(dbx.Params{"s": src}).Row(&horizon)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, false, err
		}
		if horizon.Valid && after < horizon.Int64 {
			snapshotRequired = true
			continue
		}

		var rows []oplogRow
		err = db.NewQuery(`SELECT rowid, * FROM _repl_oplog
			WHERE src_node = {:s} AND src_seq > {:a} ORDER BY src_seq LIMIT {:l}`).
			Bind(dbx.Params{"s": src, "a": after, "l": limit}).All(&rows)
		if err != nil {
			return nil, false, err
		}
		for i := range rows {
			ops = append(ops, rows[i].toOp())
		}
		limit -= len(rows)
	}
	return ops, snapshotRequired, nil
}

// advanceVector extends the contiguous per-source vector entry for src
// as far as the oplog allows and persists it. Returns the new value.
// Sequences are scanned in bounded batches so a huge backlog never
// materializes as one big in-memory slice.
func advanceVector(db dbx.Builder, src string, current int64) (int64, error) {
	const batch = 1024
	next := current
	for {
		var seqs []int64
		err := db.NewQuery(`SELECT src_seq FROM _repl_oplog
			WHERE src_node = {:s} AND src_seq > {:c} ORDER BY src_seq LIMIT {:l}`).
			Bind(dbx.Params{"s": src, "c": next, "l": batch}).Column(&seqs)
		if err != nil {
			return current, err
		}
		contiguous := true
		for _, s := range seqs {
			if s == next+1 {
				next = s
			} else if s > next+1 {
				contiguous = false
				break
			}
		}
		if !contiguous || len(seqs) < batch {
			break
		}
	}
	if next != current {
		if err := setState(db, stateVectorPrefix+src, fmt.Sprintf("%d", next)); err != nil {
			return current, err
		}
	}
	return next, nil
}

// ---------------------------------------------------------------------
// snapshot seen-set (persisted reconcile bookkeeping)

// insertSyncSeen records ids observed on the snapshot source during a
// reconcile run, in multi-row batches.
func insertSyncSeen(db dbx.Builder, runID, colID string, ids []string) error {
	const chunk = 100
	for len(ids) > 0 {
		n := len(ids)
		if n > chunk {
			n = chunk
		}
		var sb strings.Builder
		sb.WriteString(`INSERT OR IGNORE INTO _repl_sync_seen (run_id, col_id, id) VALUES `)
		params := dbx.Params{"r": runID, "c": colID}
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			key := fmt.Sprintf("i%d", i)
			sb.WriteString("({:r}, {:c}, {:" + key + "})")
			params[key] = ids[i]
		}
		if _, err := db.NewQuery(sb.String()).Bind(params).Execute(); err != nil {
			return err
		}
		ids = ids[n:]
	}
	return nil
}

// deleteSyncSeen drops the seen-set of one run (or every run when
// runID is empty).
func deleteSyncSeen(db dbx.Builder, runID string) error {
	if runID == "" {
		_, err := db.NewQuery(`DELETE FROM _repl_sync_seen`).Execute()
		return err
	}
	_, err := db.NewQuery(`DELETE FROM _repl_sync_seen WHERE run_id = {:r}`).
		Bind(dbx.Params{"r": runID}).Execute()
	return err
}

// loadVector reads all persisted vector entries.
func loadVector(db dbx.Builder) (map[string]int64, error) {
	type kv struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}
	var rows []kv
	err := db.NewQuery(`SELECT key, value FROM _repl_state WHERE key LIKE 'vector.%'`).All(&rows)
	if err != nil {
		return nil, err
	}
	vec := make(map[string]int64, len(rows))
	for _, r := range rows {
		var seq int64
		fmt.Sscanf(r.Value, "%d", &seq)
		vec[r.Key[len(stateVectorPrefix):]] = seq
	}
	return vec, nil
}

// ---------------------------------------------------------------------
// versions (LWW state)

func getVersion(db dbx.Builder, colID, recordID string) (*versionRow, error) {
	var row versionRow
	err := db.NewQuery(`SELECT * FROM _repl_versions WHERE col_id = {:c} AND record_id = {:r}`).
		Bind(dbx.Params{"c": colID, "r": recordID}).One(&row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func upsertVersion(db dbx.Builder, colID, recordID, hlcStr, srcNode string, deleted bool) error {
	del := 0
	if deleted {
		del = 1
	}
	_, err := db.NewQuery(`INSERT INTO _repl_versions (col_id, record_id, hlc, src_node, deleted, updated)
		VALUES ({:c}, {:r}, {:h}, {:s}, {:d}, {:u})
		ON CONFLICT(col_id, record_id) DO UPDATE SET
			hlc = {:h}, src_node = {:s}, deleted = {:d}, updated = {:u}`).
		Bind(dbx.Params{"c": colID, "r": recordID, "h": hlcStr, "s": srcNode, "d": del, "u": nowStr()}).
		Execute()
	return err
}

// ---------------------------------------------------------------------
// members

func listMembers(db dbx.Builder, includeRemoved bool) ([]*member, error) {
	q := `SELECT * FROM _repl_members`
	if !includeRemoved {
		q += ` WHERE removed = 0`
	}
	var rows []*member
	if err := db.NewQuery(q + ` ORDER BY node_id`).All(&rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func getMember(db dbx.Builder, nodeID string) (*member, error) {
	var m member
	err := db.NewQuery(`SELECT * FROM _repl_members WHERE node_id = {:n}`).
		Bind(dbx.Params{"n": nodeID}).One(&m)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func upsertMember(db dbx.Builder, m *member) error {
	reach, rem := 0, 0
	if m.Reachable {
		reach = 1
	}
	if m.Removed {
		rem = 1
	}
	if m.JoinedAt == "" {
		m.JoinedAt = nowStr()
	}
	_, err := db.NewQuery(`INSERT INTO _repl_members (node_id, url, reachable, joined_at, last_seen, removed)
		VALUES ({:n}, {:u}, {:re}, {:j}, {:ls}, {:rm})
		ON CONFLICT(node_id) DO UPDATE SET
			url = {:u}, reachable = {:re}, last_seen = {:ls}, removed = {:rm}`).
		Bind(dbx.Params{
			"n": m.NodeID, "u": m.URL, "re": reach,
			"j": m.JoinedAt, "ls": m.LastSeen, "rm": rem,
		}).Execute()
	return err
}

func touchMember(db dbx.Builder, nodeID string) error {
	_, err := db.NewQuery(`UPDATE _repl_members SET last_seen = {:ls}, removed = 0 WHERE node_id = {:n}`).
		Bind(dbx.Params{"ls": nowStr(), "n": nodeID}).Execute()
	return err
}
