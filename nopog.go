package nopog

import (
	"database/sql"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
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
	Created time.Time
	Updated sql.NullTime
}

type Entry struct {
	Key     string
	Created time.Time
	Updated sql.NullTime
	Data    json.RawMessage
}

// Storage composition of Database interface
type Storage struct {
	Name   string
	IP     string
	Client *sql.DB
	mutex  sync.RWMutex
	Active bool
}

func nanoTimestampToRFC3339NoTimezone(ts int64) string {
	unixTimeUTC := time.Unix(0, ts)
	return unixTimeUTC.Format("2006-01-02 15:04:05.999999999")
}

func getQuery(path string) string {
	return "select * from public.get('" + path + "')"
}

func peekQuery(path string) string {
	return "select * from public.peek('" + path + "')"
}

func deleteQuery(path string) string {
	return "select public.del('" + path + "')"
}

func setQuery(key, value string) string {
	return "select public.set('" + key + "', '" + value + "')"
}

// Start the storage client
func (db *Storage) Start() error {
	var err error
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.Name == "" || db.IP == "" {
		panic("can't connect to PgSQL without IP and database values name defined")
	}

	// TODO: receive more details about connection by params
	var conninfo string = "host=" + db.IP + " user=idx dbname=" + db.Name + " sslmode=disable"
	// log.Println("connecting to", conninfo)
	db.Client, err = sql.Open("postgres", conninfo)
	if err != nil {
		log.Println("failed to connect to pgsql", err)
		panic(err)
	}

	db.Active = true
	return err
}

// Close the storage client
func (db *Storage) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.Active = false
	db.Client.Close()
}

// Clear all keys in the storage
func (db *Storage) Clear() {
	_, err := db.Client.Exec(deleteQuery("*") + ";")
	if err != nil {
		log.Println("failed clear on sql", err)
	}
}

// Keys list all the keys in the storage
func (db *Storage) Keys() ([]string, error) {
	keys := []string{}
	rows, err := db.Client.Query(peekQuery("*") + ";")
	if err != nil {
		log.Println("failed peek keys on sql", err)
		return keys, err
	}
	defer rows.Close()

	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Created, &entry.Key, &entry.Updated)
		if err != nil {
			log.Println("failed to parse sql key entry", err)
			continue
		}
		keys = append(keys, entry.Key)
	}

	return keys, nil
}

// KeysRange list keys in a path and time range
func (db *Storage) KeysRange(path string, from, to int64, limit int) ([]string, error) {
	keys := []string{}
	now := time.Now().UTC().UnixNano()
	if to == 0 {
		to = now
	}
	timeRange := "WHERE kv.created >= '" + nanoTimestampToRFC3339NoTimezone(from) + "' AND kv.created <= '" + nanoTimestampToRFC3339NoTimezone(to) + "'"
	limitQuery := "limit " + strconv.FormatInt(int64(limit), 10)
	qry := peekQuery(path) + " AS kv " + timeRange + " " + limitQuery + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry)
	if err != nil {
		log.Println("failed get on sql", err)
		return keys, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry Key
		err = rows.Scan(&entry.Created, &entry.Key, &entry.Updated)
		if err != nil {
			log.Println("failed to parse sql entry", path, err)
			continue
		}

		keys = append(keys, entry.Key)
	}

	return keys, nil
}

// Get a key/pattern related value(s)
func (db *Storage) Get(path string) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getQuery(path) + ";")
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
			updatedTime = entry.Updated.Time.UnixNano()
		}

		res = append(res, Object{
			Created: entry.Created.UnixNano(),
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	return res, nil
}

// GetN get last N elements of a pattern related value(s)
func (db *Storage) GetN(path string, limit int) ([]Object, error) {
	res := []Object{}
	rows, err := db.Client.Query(getQuery(path) + " limit " + strconv.FormatInt(int64(limit), 10) + ";")
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
			updatedTime = entry.Updated.Time.UnixNano()
		}

		res = append(res, Object{
			Created: entry.Created.UnixNano(),
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	return res, nil
}

// GetNRange get last N elements of a pattern related value(s) in a time range. "to = 0" is treated as now
func (db *Storage) GetNRange(path string, from, to int64, limit int) ([]Object, error) {
	res := []Object{}
	now := time.Now().UTC().UnixNano()
	if to == 0 {
		to = now
	}
	timeRange := "WHERE kv.created >= '" + nanoTimestampToRFC3339NoTimezone(from) + "' AND kv.created <= '" + nanoTimestampToRFC3339NoTimezone(to) + "'"
	limitQuery := "limit " + strconv.FormatInt(int64(limit), 10)
	qry := getQuery(path) + " AS kv " + timeRange + " " + limitQuery + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry)
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
			updatedTime = entry.Updated.Time.UnixNano()
		}

		res = append(res, Object{
			Created: entry.Created.UnixNano(),
			Updated: updatedTime,
			Key:     entry.Key,
			Value:   entry.Data,
		})
	}

	return res, nil
}

// Set a value
func (db *Storage) Set(key string, value string) (string, error) {
	_, err := db.Client.Exec(setQuery(key, value) + ";")
	if err != nil {
		return "", err
	}

	return key, nil
}

// Del a key/pattern value(s)
func (db *Storage) Del(path string) error {
	_, err := db.Client.Exec(deleteQuery(path) + ";")
	if err != nil {
		log.Println("failed del on sql", path, err)
		return err

	}

	return nil
}
