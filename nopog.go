package nopog

import (
	"database/sql"
	"encoding/json"
	"log"
	"strconv"
	"strings"
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
	Name     string
	User     string
	Password string
	SSLMode  string
	Host     string
	Port     string
	Client   *sql.DB
	mutex    sync.RWMutex
	listener *pq.Listener
	Active   bool
}

type BroadcastEvent struct {
	Key string
	OP  string
}

const NOTIMEZONE = "2006-01-02 15:04:05.999999999"

func nanoTimestampToRFC3339NoTimezone(ts int64) string {
	unixTimeUTC := time.Unix(0, ts)
	return unixTimeUTC.Format(NOTIMEZONE)
}

// since we are using timestamp with no timezone the scan of nulltime results in UTC timezone
// this functions replaces the default UTC timezone with the local one
func removeUTCTimezoneFromTime(dt time.Time) (time.Time, error) {
	process := strings.Replace(dt.String(), " +0000 +0000", "", 1)
	return time.ParseInLocation(NOTIMEZONE, process, time.Now().Location())
}

func getQuery() string {
	return "select * from public.get($1)"
}

func peekQuery() string {
	return "select * from public.peek($1)"
}

func deleteQuery() string {
	return "select public.del($1)"
}

func setQuery() string {
	return "select public.set($1, $2)"
}

// Start the storage client
func (db *Storage) Start() error {
	var err error
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.Name == "" || db.Host == "" {
		panic("can't connect to PgSQL without IP and database values name defined")
	}

	if db.SSLMode == "" {
		db.SSLMode = "disable"
	}

	var conninfo string = "host=" + db.Host + " user=" + db.User + " dbname=" + db.Name + " sslmode=" + db.SSLMode
	if db.Password != "" {
		conninfo += " password=" + db.Password
	}

	if db.Port != "" && db.Port != "5432" {
		conninfo += " port=" + db.Port
	}

	log.Println("connecting to ", db.Host)
	db.Client, err = sql.Open("postgres", conninfo)
	if err != nil {
		log.Println("failed to connect to pgsql", err)
		panic(err)
	}

	// sample of notify listener
	// go db.Listen(conninfo)

	db.Active = true
	return err
}

// Close the storage client
func (db *Storage) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.Active = false
	// db.listener.Close()
	db.Client.Close()
}

// Clear all keys in the storage
func (db *Storage) Clear() {
	_, err := db.Client.Exec(deleteQuery()+";", "*")
	if err != nil {
		log.Println("failed clear on sql", err)
	}
}

// Keys list all the keys in the storage
func (db *Storage) Keys() ([]string, error) {
	keys := []string{}
	rows, err := db.Client.Query(peekQuery()+";", "*")
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
	timeRange := "WHERE kv.created >= $2 AND kv.created <= $3"
	limitQuery := "limit $4"
	qry := peekQuery() + " AS kv " + timeRange + " " + limitQuery + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry, path, nanoTimestampToRFC3339NoTimezone(from), nanoTimestampToRFC3339NoTimezone(to), strconv.FormatInt(int64(limit), 10))
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
	rows, err := db.Client.Query(getQuery()+";", path)
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
	rows, err := db.Client.Query(getQuery()+" limit $2;", path, strconv.FormatInt(int64(limit), 10))
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

// GetNRange get last N elements of a pattern related value(s) created in a time range. "to = 0" is treated as now
func (db *Storage) GetNRange(path string, from, to int64, limit int) ([]Object, error) {
	res := []Object{}
	now := time.Now().UnixNano()
	if to == 0 {
		to = now
	}
	timeRange := "WHERE kv.created >= $2 AND kv.created <= $3"
	limitQuery := "limit $4"
	qry := getQuery() + " AS kv " + timeRange + " " + limitQuery + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry, path, nanoTimestampToRFC3339NoTimezone(from), nanoTimestampToRFC3339NoTimezone(to), strconv.FormatInt(int64(limit), 10))
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

// GetRange get elements of a pattern related value(s) created in a time range. "to = 0" is treated as now
func (db *Storage) GetRange(path string, from, to int64) ([]Object, error) {
	res := []Object{}
	now := time.Now().UnixNano()
	if to == 0 {
		to = now
	}
	timeRange := "WHERE kv.created >= $2 AND kv.created <= $3"
	qry := getQuery() + " AS kv " + timeRange + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry, path, nanoTimestampToRFC3339NoTimezone(from), nanoTimestampToRFC3339NoTimezone(to))
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

// GetUpdatedRange get elements of a pattern related value(s) updated in a time range. "to = 0" is treated as now
func (db *Storage) GetUpdatedRange(path string, from, to int64) ([]Object, error) {
	res := []Object{}
	now := time.Now().UnixNano()
	if to == 0 {
		to = now
	}
	timeRange := "WHERE kv.updated >= $2 AND kv.updated <= $3"
	qry := getQuery() + " AS kv " + timeRange + ";"
	// log.Println(qry)
	rows, err := db.Client.Query(qry, path, nanoTimestampToRFC3339NoTimezone(from), nanoTimestampToRFC3339NoTimezone(to))
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
func (db *Storage) Set(key string, value string) (int64, error) {
	entryTime := int64(0)

	res, err := db.Client.Query(setQuery()+";", key, value)
	if err != nil {
		return entryTime, err
	}
	defer res.Close()

	var opTime sql.NullTime
	res.Next()
	err = res.Scan(&opTime)
	if err != nil {
		return entryTime, err
	}

	if !opTime.Valid {
		return entryTime, err
	}

	localTime, err := removeUTCTimezoneFromTime(opTime.Time)
	if err != nil {
		return entryTime, err
	}

	entryTime = localTime.UnixNano()

	return entryTime, nil
}

func logListener(event pq.ListenerEventType, err error) {
	if err != nil {
		log.Println("listener error: ", err)
		panic(err)
	}
	if event == pq.ListenerEventConnectionAttemptFailed {
		log.Println("failed to listen", err)
		panic(err)
	}
}

func (db *Storage) Listen(conn string) {
	db.listener = pq.NewListener(conn,
		10*time.Second, time.Minute,
		logListener)

	err := db.listener.Listen("broadcast")
	if err != nil {
		db.listener.Close()
		panic(err)
	}

	for {
		select {
		case e := <-db.listener.Notify:
			if e == nil {
				continue
			}
			var event BroadcastEvent
			err := json.Unmarshal([]byte(e.Extra), &event)
			if err != nil {
				log.Println("failed to parse broadcast event", err)
				continue
			}
			log.Println("broadcast", event.Key, event.OP)
		case <-time.After(time.Minute):
			go db.listener.Ping()
		}
	}
}

// Del a key/pattern value(s)
func (db *Storage) Del(path string) error {
	_, err := db.Client.Exec(deleteQuery()+";", path)
	if err != nil {
		log.Println("failed del on sql", path, err)
		return err

	}

	return nil
}
