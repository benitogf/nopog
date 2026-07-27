package nopog

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/benitogf/coat"
	"github.com/lib/pq"
)

// SchemaSQL is the canonical nopog.sql schema, embedded at build time so it can
// be applied programmatically (see the install tool) without a separate file.
//
//go:embed nopog.sql
var SchemaSQL string

// Object : data structure of elements
type Object struct {
	Created int64           `json:"created"`
	Updated int64           `json:"updated"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
}

type Key struct {
	Key     string
	Created int64
	Updated sql.NullInt64
}

type Entry struct {
	Key     string
	Created int64
	Updated sql.NullInt64
	Data    json.RawMessage
}

// Storage composition of Database interface
type Storage struct {
	Name            string
	User            string
	Password        string
	SSLMode         string
	Host            string
	Port            string
	MaxRetries      int           // Maximum connection retries (default 120)
	MaxOpenConns    int           // Maximum open connections (default 50)
	MaxIdleConns    int           // Maximum idle connections (default 25)
	ConnMaxLifetime time.Duration // Connection max lifetime (default 5 minutes)
	ConnMaxIdleTime time.Duration // Connection max idle time (default 1 minute)
	Client          *sql.DB
	mutex           sync.RWMutex
	Active          bool
	Silence         bool          // Silence output flag
	Console         *coat.Console // Logging console
}

func getQuery() string {
	return "select * from public.nopog_get($1, $2)"
}

func getRangeQuery() string {
	return "select * from public.nopog_get_range($1, $2, $3, $4, $5)"
}

func peekQuery() string {
	return "select * from public.nopog_peek($1, $2)"
}

func peekRangeQuery() string {
	return "select * from public.nopog_peek_range($1, $2, $3, $4, $5)"
}

func deleteQuery() string {
	return "select public.nopog_del($1, $2)"
}

func setQuery() string {
	return "select public.nopog_set($1, $2, $3)"
}

func createTableQuery() string {
	return "select public.create_table($1)"
}

func dropTableQuery() string {
	return "select public.drop_table($1)"
}

func getByJSONQuery() string {
	return "select * from public.nopog_get_by_json($1, $2, $3)"
}

func getByFieldQuery() string {
	return "select * from public.nopog_get_by_field($1, $2, $3, $4)"
}

// Start the storage client with retry logic and ping verification
func (db *Storage) Start() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Initialize console if not set
	if db.Console == nil {
		db.Console = coat.NewConsole("nopog:"+db.Name, db.Silence)
	}

	if db.Name == "" || db.Host == "" {
		return fmt.Errorf("can't connect to PgSQL without Host and Name defined")
	}

	if db.SSLMode == "" {
		db.SSLMode = "disable"
	}

	maxRetries := db.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 120
	}

	conninfo := "host=" + db.Host + " user=" + db.User + " dbname=" + db.Name + " sslmode=" + db.SSLMode
	if db.Password != "" {
		conninfo += " password=" + db.Password
	}

	if db.Port != "" && db.Port != "5432" {
		conninfo += " port=" + db.Port
	}

	db.Console.Log("Start: connecting to", db.Host)

	// Connection retry loop
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		db.Client, err = sql.Open("postgres", conninfo)
		if err == nil {
			break
		}
		db.Console.Err("Start: failed to connect to sql", db.Host, db.Port, "attempt", attempt, "/", maxRetries, err)
		if attempt == maxRetries {
			return fmt.Errorf("failed to connect to sql db after %d retries: %w", maxRetries, err)
		}
		time.Sleep(1 * time.Second)
	}

	// Ping retry loop
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = db.Client.Ping()
		if err == nil {
			break
		}
		db.Console.Err("Start: failed to ping sql", db.Host, db.Port, "attempt", attempt, "/", maxRetries, err)
		if attempt == maxRetries {
			db.Client.Close()
			return fmt.Errorf("failed to ping sql db after %d retries: %w", maxRetries, err)
		}
		time.Sleep(1 * time.Second)
	}

	// Configure connection pool with sensible defaults for high load
	maxOpenConns := db.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = 50 // Good balance for high concurrency without overwhelming DB
	}
	maxIdleConns := db.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = 25 // Keep connections warm for burst traffic
	}
	connMaxLifetime := db.ConnMaxLifetime
	if connMaxLifetime <= 0 {
		connMaxLifetime = 5 * time.Minute // Recycle to prevent stale connections
	}
	connMaxIdleTime := db.ConnMaxIdleTime
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 1 * time.Minute // Release idle connections faster
	}

	db.Client.SetMaxOpenConns(maxOpenConns)
	db.Client.SetMaxIdleConns(maxIdleConns)
	db.Client.SetConnMaxLifetime(connMaxLifetime)
	db.Client.SetConnMaxIdleTime(connMaxIdleTime)

	db.Active = true
	return nil
}

// Close the storage client
func (db *Storage) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.Active = false
	db.Client.Close()
}

// CreateTable creates a new table pair (keys_<table>, values_<table>)
func (db *Storage) CreateTable(table string) error {
	_, err := db.Client.Exec(createTableQuery()+";", table)
	if err != nil {
		db.Console.Err("CreateTable: failed to create table", table, err)
		return err
	}
	return nil
}

// DropTable drops a table pair (keys_<table>, values_<table>)
func (db *Storage) DropTable(table string) error {
	_, err := db.Client.Exec(dropTableQuery()+";", table)
	if err != nil {
		db.Console.Err("DropTable: failed to drop table", table, err)
		return err
	}
	return nil
}

// Clear all keys in a table
func (db *Storage) Clear(table string) {
	_, err := db.Client.Exec(deleteQuery()+";", table, "*")
	if err != nil {
		db.Console.Err("Clear: failed clear on sql", err)
	}
}

// Keys list all the keys in a table
func (db *Storage) Keys(table string) ([]string, error) {
	keys := []string{}
	rows, err := db.Client.Query(peekQuery()+";", table, "*")
	if err != nil {
		db.Console.Err("Keys: failed peek keys on sql", err)
		return keys, err
	}
	defer rows.Close()

	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated)
		if err != nil {
			return keys, err
		}
		keys = append(keys, entry.Key)
	}

	if err := rows.Err(); err != nil {
		return keys, err
	}
	return keys, nil
}

// KeysRange list keys in a path and time range. "to = 0" is now; a limit <= 0 means no limit.
func (db *Storage) KeysRange(table, path string, from, to int64, limit int) ([]string, error) {
	keys := []string{}
	rows, err := db.Client.Query(peekRangeQuery()+";", table, path, from, to, limit)
	if err != nil {
		db.Console.Err("KeysRange: failed get on sql", err)
		return keys, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated)
		if err != nil {
			return keys, err
		}

		keys = append(keys, entry.Key)
	}

	if err := rows.Err(); err != nil {
		return keys, err
	}
	return keys, nil
}

// Get a key/pattern related value(s) from a table
// Supports an exact key or a single trailing glob (fast, uses prefix index).
// Examples: "users/1" (exact), "users/*" (single glob).
// Multi-glob patterns (e.g. "a/*/b") are rejected as invalid keys.
// Timestamps are microseconds since epoch; equal created values are possible
// within the same µs, and ordering tiebreaks by key.
func (db *Storage) Get(table, path string) ([]Object, error) {
	res := []Object{}

	rows, err := db.Client.Query(getQuery()+";", table, path)
	if err != nil {
		db.Console.Err("Get: failed get on sql", path, err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// GetN get last N elements of a pattern related value(s) from a table.
// A limit <= 0 means no limit (all matching elements), consistent with the range API.
func (db *Storage) GetN(table, path string, limit int) ([]Object, error) {
	res := []Object{}
	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = db.Client.Query(getQuery()+" limit $3;", table, path, strconv.FormatInt(int64(limit), 10))
	} else {
		rows, err = db.Client.Query(getQuery()+";", table, path)
	}
	if err != nil {
		db.Console.Err("GetN: failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// GetNRange get last N elements of a pattern related value(s) created in a time range. "to = 0" is treated as now.
// Time bounds are microseconds since epoch; ordering tiebreaks by key. A limit <= 0 means no limit.
func (db *Storage) GetNRange(table, path string, from, to int64, limit int) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getRangeQuery()+";", table, path, from, to, limit)
	if err != nil {
		db.Console.Err("GetNRange: failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// GetRange get elements of a pattern related value(s) created in a time range. "to = 0" is treated as now.
// Time bounds are microseconds since epoch; ordering tiebreaks by key.
// Passes limit 0 (no limit) to return all matching results in the range.
func (db *Storage) GetRange(table, path string, from, to int64) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getRangeQuery()+";", table, path, from, to, 0)
	if err != nil {
		db.Console.Err("GetRange: failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// Set a value in a table, returning the created timestamp in microseconds since
// epoch. Equal timestamps are possible for writes within the same microsecond;
// ordering and cursors tiebreak by key.
func (db *Storage) Set(table, key, value string) (int64, error) {
	entryTime := int64(0)

	res, err := db.Client.Query(setQuery()+";", table, key, value)
	if err != nil {
		return entryTime, err
	}
	defer res.Close()

	res.Next()
	err = res.Scan(&entryTime)
	if err != nil {
		return entryTime, err
	}

	return entryTime, nil
}

// SetBatch inserts multiple key-value pairs in a single database call for better performance
func (db *Storage) SetBatch(table string, keys []string, values []string) ([]int64, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("keys and values must have same length")
	}
	if len(keys) == 0 {
		return []int64{}, nil
	}

	res, err := db.Client.Query("select public.nopog_set_batch($1, $2, $3);", table, pq.Array(keys), pq.Array(values))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var timestamps []int64
	if res.Next() {
		err = res.Scan(pq.Array(&timestamps))
		if err != nil {
			return nil, err
		}
	} else if err := res.Err(); err != nil {
		// A false Next() with an error is a mid-stream/transport failure, not an
		// empty result — surface it rather than returning nil timestamps as success.
		return nil, err
	}

	return timestamps, nil
}

// SetWithMeta sets a value with caller-supplied created/updated timestamps,
// overwriting both on conflict. Timestamps are microseconds since epoch.
func (db *Storage) SetWithMeta(table, key, value string, created, updated int64) error {
	_, err := db.Client.Exec("select public.nopog_set_meta($1, $2, $3, $4, $5);", table, key, value, created, updated)
	if err != nil {
		db.Console.Err("SetWithMeta: failed set_meta on sql", key, err)
		return err
	}
	return nil
}

// ImportBatch additively imports entries; existing rows always win. An entry
// with Updated == 0 is stored as a NULL updated. Returns the number of rows
// actually inserted (genuinely new keys). Timestamps are microseconds since epoch.
func (db *Storage) ImportBatch(table string, entries []Object) (inserted int, err error) {
	if len(entries) == 0 {
		return 0, nil
	}

	const chunkSize = 1000
	for start := 0; start < len(entries); start += chunkSize {
		end := start + chunkSize
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[start:end]

		keys := make([]string, len(chunk))
		created := make([]int64, len(chunk))
		updated := make([]sql.NullInt64, len(chunk))
		values := make([]string, len(chunk))
		for i, e := range chunk {
			keys[i] = e.Key
			created[i] = e.Created
			if e.Updated == 0 {
				updated[i] = sql.NullInt64{}
			} else {
				updated[i] = sql.NullInt64{Int64: e.Updated, Valid: true}
			}
			values[i] = string(e.Value)
		}

		var chunkInserted int
		row := db.Client.QueryRow("select public.nopog_import($1, $2, $3, $4, $5);",
			table, pq.Array(keys), pq.Array(created), pq.Array(updated), pq.Array(values))
		if err = row.Scan(&chunkInserted); err != nil {
			db.Console.Err("ImportBatch: failed import on sql", err)
			return inserted, err
		}
		inserted += chunkInserted
	}

	return inserted, nil
}

// Scan walks the table by keyset pagination ordered ascending by (created, key),
// returning up to limit rows strictly after the (cursorCreated, cursorKey) cursor.
// A limit <= 0 means no limit (all rows after the cursor).
// The cursor is stable under equal created values because it tiebreaks by key.
func (db *Storage) Scan(table string, cursorCreated int64, cursorKey string, limit int) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query("select * from public.nopog_scan($1, $2, $3, $4);", table, cursorCreated, cursorKey, limit)
	if err != nil {
		db.Console.Err("Scan: failed scan on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}
		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}
		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}
	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// GetRangeSegment returns entries in a time range whose key has any of the given
// path-segment positions (1-based, '/'-delimited) equal to value. "to = 0" is now,
// and "limit <= 0" means no limit (all matching rows in range).
func (db *Storage) GetRangeSegment(table, path string, from, to int64, limit int, positions []int, value string) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query("select * from public.nopog_get_range_segment($1, $2, $3, $4, $5, $6, $7);",
		table, path, from, to, limit, pq.Array(positions), value)
	if err != nil {
		db.Console.Err("GetRangeSegment: failed get on sql", path, err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}
		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}
		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}
	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// Del a key/pattern value(s) from a table
func (db *Storage) Del(table, path string) error {
	_, err := db.Client.Exec(deleteQuery()+";", table, path)
	if err != nil {
		db.Console.Err("Del: failed del on sql", path, err)
		return err

	}

	return nil
}

// GetByJSON queries entries where JSONB data contains the specified JSON object
// Uses GIN index for efficient containment queries (@> operator)
// Example: GetByJSON("mytable", "users/*", `{"status":"active"}`)
func (db *Storage) GetByJSON(table, path, jsonFilter string) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getByJSONQuery()+";", table, path, jsonFilter)
	if err != nil {
		db.Console.Err("GetByJSON: failed get by json on sql", path, err)
		return res, err
	}
	defer rows.Close()

	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// GetByField queries entries where a specific JSONB field equals a value
// Example: GetByField("mytable", "users/*", "status", "active")
func (db *Storage) GetByField(table, path, fieldName, fieldValue string) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getByFieldQuery()+";", table, path, fieldName, fieldValue)
	if err != nil {
		db.Console.Err("GetByField: failed get by field on sql", path, err)
		return res, err
	}
	defer rows.Close()

	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			return res, err
		}

		updatedTime := int64(0)
		if entry.Updated.Valid {
			updatedTime = entry.Updated.Int64
		}

		res = append(res, Object{
			Created: entry.Created,
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	if err := rows.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// TableStatistics contains statistics about a table
type TableStatistics struct {
	RowCount  int64 // Number of live rows
	SizeBytes int64 // Total size in bytes (keys + values tables)
}

// TableStats returns statistics for a table to help determine if partitioning is needed
// Recommended thresholds: RowCount > 10M or SizeBytes > 10GB suggests partitioning
func (db *Storage) TableStats(table string) (*TableStatistics, error) {
	var stats TableStatistics
	keysTable := "keys_" + table
	valuesTable := "values_" + table

	// Get row count from pg_stat_user_tables
	err := db.Client.QueryRow(`
		SELECT COALESCE(n_live_tup, 0) 
		FROM pg_stat_user_tables 
		WHERE relname = $1
	`, keysTable).Scan(&stats.RowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// Get total size (keys + values tables)
	var keysSize, valuesSize int64
	err = db.Client.QueryRow(`SELECT pg_total_relation_size($1)`, "public."+keysTable).Scan(&keysSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys table size: %w", err)
	}
	err = db.Client.QueryRow(`SELECT pg_total_relation_size($1)`, "public."+valuesTable).Scan(&valuesSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get values table size: %w", err)
	}
	stats.SizeBytes = keysSize + valuesSize

	return &stats, nil
}
