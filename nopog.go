package nopog

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/benitogf/coat"
	"github.com/lib/pq"
)

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

func getMultiGlobQuery() string {
	return "select * from public.nopog_get_multiglob($1, $2)"
}

// isMultiGlob detects if a pattern has multiple wildcards or wildcard in middle
// Returns true for patterns like "a/*/b/*" or "a/*/b" that need regex matching
func isMultiGlob(pattern string) bool {
	wildcardCount := 0
	lastWildcardPos := -1
	for i, c := range pattern {
		if c == '*' {
			wildcardCount++
			lastWildcardPos = i
		}
	}
	if wildcardCount > 1 {
		return true
	}
	if wildcardCount == 1 && lastWildcardPos != len(pattern)-1 {
		return true
	}
	return false
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
			db.Console.Err("Keys: failed to parse sql key entry", err)
			continue
		}
		keys = append(keys, entry.Key)
	}

	return keys, nil
}

// KeysRange list keys in a path and time range
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
			db.Console.Err("KeysRange: failed to parse sql entry", path, err)
			continue
		}

		keys = append(keys, entry.Key)
	}

	return keys, nil
}

// Get a key/pattern related value(s) from a table
// Supports single glob at end (fast, uses prefix index) and multi-glob patterns (slower, uses regex)
// Examples: "users/*" (single glob), "stats/*/data/*" (multi-glob)
func (db *Storage) Get(table, path string) ([]Object, error) {
	res := []Object{}

	// Auto-detect multi-glob patterns and use appropriate query
	var rows *sql.Rows
	var err error
	if isMultiGlob(path) {
		rows, err = db.Client.Query(getMultiGlobQuery()+";", table, path)
	} else {
		rows, err = db.Client.Query(getQuery()+";", table, path)
	}

	if err != nil {
		db.Console.Err("Get: failed get on sql", path, err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			db.Console.Err("Get: failed to parse sql entry", path, err)
			continue
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

	return res, nil
}

// GetN get last N elements of a pattern related value(s) from a table
func (db *Storage) GetN(table, path string, limit int) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getQuery()+" limit $3;", table, path, strconv.FormatInt(int64(limit), 10))
	if err != nil {
		db.Console.Err("GetN: failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			db.Console.Err("GetN: failed to parse sql key entry", err)
			continue
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

	return res, nil
}

// GetNRange get last N elements of a pattern related value(s) created in a time range. "to = 0" is treated as now
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
			db.Console.Err("GetNRange: failed to parse sql entry", path, err)
			continue
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

	return res, nil
}

// GetRange get elements of a pattern related value(s) created in a time range. "to = 0" is treated as now
// Uses a high limit (1 billion) to effectively get all results
func (db *Storage) GetRange(table, path string, from, to int64) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getRangeQuery()+";", table, path, from, to, 1000000000)
	if err != nil {
		db.Console.Err("GetRange: failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			db.Console.Err("GetRange: failed to parse sql entry", path, err)
			continue
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

	return res, nil
}

// Set a value in a table
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
	}

	return timestamps, nil
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
			db.Console.Err("GetByJSON: failed to parse sql entry", path, err)
			continue
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
			db.Console.Err("GetByField: failed to parse sql entry", path, err)
			continue
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
