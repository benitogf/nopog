package nopog

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

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
	Name       string
	User       string
	Password   string
	SSLMode    string
	Host       string
	Port       string
	MaxRetries int // Maximum connection retries (default 120)
	Client     *sql.DB
	mutex      sync.RWMutex
	Active     bool
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

// Start the storage client with retry logic and ping verification
func (db *Storage) Start() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

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

	log.Println("connecting to ", db.Host)

	// Connection retry loop
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		db.Client, err = sql.Open("postgres", conninfo)
		if err == nil {
			break
		}
		log.Printf("failed to connect to sql %s:%s (attempt %d/%d): %v", db.Host, db.Port, attempt, maxRetries, err)
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
		log.Printf("failed to ping sql %s:%s (attempt %d/%d): %v", db.Host, db.Port, attempt, maxRetries, err)
		if attempt == maxRetries {
			db.Client.Close()
			return fmt.Errorf("failed to ping sql db after %d retries: %w", maxRetries, err)
		}
		time.Sleep(1 * time.Second)
	}

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
		log.Println("failed to create table", table, err)
		return err
	}
	return nil
}

// DropTable drops a table pair (keys_<table>, values_<table>)
func (db *Storage) DropTable(table string) error {
	_, err := db.Client.Exec(dropTableQuery()+";", table)
	if err != nil {
		log.Println("failed to drop table", table, err)
		return err
	}
	return nil
}

// Clear all keys in a table
func (db *Storage) Clear(table string) {
	_, err := db.Client.Exec(deleteQuery()+";", table, "*")
	if err != nil {
		log.Println("failed clear on sql", err)
	}
}

// Keys list all the keys in a table
func (db *Storage) Keys(table string) ([]string, error) {
	keys := []string{}
	rows, err := db.Client.Query(peekQuery()+";", table, "*")
	if err != nil {
		log.Println("failed peek keys on sql", err)
		return keys, err
	}
	defer rows.Close()

	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated)
		if err != nil {
			log.Println("failed to parse sql key entry", err)
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
		log.Println("failed get on sql", err)
		return keys, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated)
		if err != nil {
			log.Println("failed to parse sql entry", path, err)
			continue
		}

		keys = append(keys, entry.Key)
	}

	return keys, nil
}

// Get a key/pattern related value(s) from a table
func (db *Storage) Get(table, path string) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getQuery()+";", table, path)
	if err != nil {
		log.Println("failed get on sql", path, err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			log.Println("failed to parse sql entry", path, err)
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
		log.Println("failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			log.Println("failed to parse sql key entry", err)
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
		log.Println("failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			log.Println("failed to parse sql entry", path, err)
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
		log.Println("failed get on sql", err)
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Entry
		err = rows.Scan(&entry.Key, &entry.Created, &entry.Updated, &entry.Data)
		if err != nil {
			log.Println("failed to parse sql entry", path, err)
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
		log.Println("failed del on sql", path, err)
		return err

	}

	return nil
}
