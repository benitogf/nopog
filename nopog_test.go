package nopog

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testObject string = `{"ob":"test ✔⚓🛸🛴"}`
var testKey string = "test/"
var testTable string = "default"

var testServerIP string = "localhost"
var testServerDatabase string = "postgres"
var testServerUser = "postgres"
var testServerPassword = "postgres"

func newTestStorage(t *testing.T) *Storage {
	storage := &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
		Silence:  true,
	}
	err := storage.Start()
	require.NoError(t, err)
	// Drop and recreate table to ensure JSONB schema
	storage.DropTable(testTable)
	err = storage.CreateTable(testTable)
	require.NoError(t, err)
	storage.Clear(testTable)
	return storage
}

func TestKeys(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()
	keys, err := storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, []string{}, keys)
	_, err = storage.Set(testTable, testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "notValidKey//", testObject)
	require.Error(t, err)
	_, err = storage.Set(testTable, testKey+"1", `not json`)
	require.Error(t, err)
	keys, err = storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, []string{testKey + "1"}, keys)
}

func TestSetAndGet(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()
	key := testKey + "1"
	_, err := storage.Set(testTable, key, testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, key, `not json`)
	require.Error(t, err)
	dataList, err := storage.Get(testTable, key)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, key, data.Key)
	require.JSONEq(t, testObject, string(data.Value))
}

func TestGetPath(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()
	_, err := storage.Set(testTable, testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.Get(testTable, testKey+"*")
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.JSONEq(t, testObject, string(data.Value))
}

func TestGetN(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()
	_, err := storage.Set(testTable, testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.GetN(testTable, testKey+"*", 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.JSONEq(t, testObject, string(data.Value))
}

func TestRange(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()
	firstOpTime, err := storage.Set(testTable, testKey+"1", testObject)
	require.NoError(t, err)
	secondOpTime, err := storage.Set(testTable, testKey+"2", testObject)
	require.NoError(t, err)

	// Monotonic timestamps guarantee secondOpTime > firstOpTime
	require.Greater(t, secondOpTime, firstOpTime)

	// Use a range that includes both entries
	dataList, err := storage.GetNRange(testTable, testKey+"*", firstOpTime, secondOpTime, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	// Results ordered by created DESC (most recent first)
	require.Equal(t, testKey+"2", dataList[0].Key)
	require.JSONEq(t, testObject, string(dataList[0].Value))
	require.Equal(t, testKey+"1", dataList[1].Key)
	require.JSONEq(t, testObject, string(dataList[1].Value))

	// Get only the second entry using its timestamp as lower bound
	dataList, err = storage.GetNRange(testTable, testKey+"*", secondOpTime, 0, 2)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, testKey+"2", dataList[0].Key)
	require.JSONEq(t, testObject, string(dataList[0].Value))

	// KeysRange with range including both
	keys, err := storage.KeysRange(testTable, testKey+"*", firstOpTime, secondOpTime, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(keys))
	require.Equal(t, testKey+"2", keys[0])
	require.Equal(t, testKey+"1", keys[1])

	// GetRange with range including both
	dataList, err = storage.GetRange(testTable, testKey+"*", firstOpTime, secondOpTime)
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	require.Equal(t, testKey+"2", dataList[0].Key)
	require.Equal(t, testKey+"1", dataList[1].Key)
}

func TestValidGlobPatterns(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Valid: single glob at end of path
	_, err := storage.Set(testTable, "items/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "items/2", testObject)
	require.NoError(t, err)

	// Get with valid glob pattern (single * at end)
	dataList, err := storage.Get(testTable, "items/*")
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))

	// Get all with *
	dataList, err = storage.Get(testTable, "*")
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
}

func TestInvalidGlobPatterns(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Note: multiple globs and glob in middle are now VALID (multi-glob support)
	// These patterns now use regex matching instead of prefix matching

	// Invalid: double glob (**) - still invalid
	_, err := storage.Get(testTable, "items/**")
	require.Error(t, err)

	// Invalid: double separator
	_, err = storage.Get(testTable, "items//test")
	require.Error(t, err)
}

func TestPrefixMatching(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create entries at different depths
	_, err := storage.Set(testTable, "a/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/b/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/b/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/b/c/1", testObject)
	require.NoError(t, err)

	// Prefix matching returns all descendants (no depth enforcement)
	// a/* returns all keys starting with "a/"
	dataList, err := storage.Get(testTable, "a/*")
	require.NoError(t, err)
	require.Equal(t, 5, len(dataList)) // All 5 entries start with "a/"

	// a/b/* returns all keys starting with "a/b/"
	dataList, err = storage.Get(testTable, "a/b/*")
	require.NoError(t, err)
	require.Equal(t, 3, len(dataList)) // a/b/1, a/b/2, a/b/c/1

	// a/b/c/* returns all keys starting with "a/b/c/"
	dataList, err = storage.Get(testTable, "a/b/c/*")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, "a/b/c/1", dataList[0].Key)
}

func TestDeleteWithGlob(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create entries
	_, err := storage.Set(testTable, "del/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "del/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "del/sub/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "keep/1", testObject)
	require.NoError(t, err)

	// Delete with glob deletes all descendants with matching prefix
	err = storage.Del(testTable, "del/*")
	require.NoError(t, err)

	// Verify all del/* entries are deleted (including nested)
	dataList, err := storage.Get(testTable, "del/1")
	require.NoError(t, err)
	require.Equal(t, 0, len(dataList))

	dataList, err = storage.Get(testTable, "del/2")
	require.NoError(t, err)
	require.Equal(t, 0, len(dataList))

	dataList, err = storage.Get(testTable, "del/sub/1")
	require.NoError(t, err)
	require.Equal(t, 0, len(dataList)) // Also deleted (prefix match)

	// Verify keep/1 still exists
	dataList, err = storage.Get(testTable, "keep/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
}

func TestDeleteExactKey(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	_, err := storage.Set(testTable, "exact/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "exact/2", testObject)
	require.NoError(t, err)

	// Delete exact key
	err = storage.Del(testTable, "exact/1")
	require.NoError(t, err)

	// Verify exact/1 is deleted
	dataList, err := storage.Get(testTable, "exact/1")
	require.NoError(t, err)
	require.Equal(t, 0, len(dataList))

	// Verify exact/2 still exists
	dataList, err = storage.Get(testTable, "exact/2")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
}

func TestDeleteAll(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	_, err := storage.Set(testTable, "all/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "all/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "other/1", testObject)
	require.NoError(t, err)

	// Delete all
	err = storage.Del(testTable, "*")
	require.NoError(t, err)

	// Verify all deleted
	keys, err := storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, 0, len(keys))
}

func TestPrefixMatchingWithSimilarPrefixes(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create entries with similar prefixes
	_, err := storage.Set(testTable, "test/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "test/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "testing/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "tester/1", testObject)
	require.NoError(t, err)

	// Get test/* should only return test/1 and test/2
	dataList, err := storage.Get(testTable, "test/*")
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	for _, d := range dataList {
		require.Contains(t, []string{"test/1", "test/2"}, d.Key)
	}

	// Get testing/* should only return testing/1
	dataList, err = storage.Get(testTable, "testing/*")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, "testing/1", dataList[0].Key)
}

func TestGetNWithGlob(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert in order - GetN returns most recent first (ORDER BY created DESC)
	_, err := storage.Set(testTable, "limit/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "limit/2", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "limit/3", testObject)
	require.NoError(t, err)

	// Get with limit 2 - should return the 2 most recent entries
	dataList, err := storage.GetN(testTable, "limit/*", 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	// Most recent first (limit/3, limit/2)
	require.Equal(t, "limit/3", dataList[0].Key)
	require.JSONEq(t, testObject, string(dataList[0].Value))
	require.Equal(t, "limit/2", dataList[1].Key)
	require.JSONEq(t, testObject, string(dataList[1].Value))

	// Get with limit 1 - should return only the most recent
	dataList, err = storage.GetN(testTable, "limit/*", 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, "limit/3", dataList[0].Key)
}

func TestKeysRangeWithGlob(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	firstTime, err := storage.Set(testTable, "range/1", testObject)
	require.NoError(t, err)
	secondTime, err := storage.Set(testTable, "range/2", testObject)
	require.NoError(t, err)
	thirdTime, err := storage.Set(testTable, "range/3", testObject)
	require.NoError(t, err)

	// Monotonic timestamps guarantee ordering
	require.Greater(t, secondTime, firstTime)
	require.Greater(t, thirdTime, secondTime)

	// Get keys in range that includes only second entry (using secondTime as both bounds)
	keys, err := storage.KeysRange(testTable, "range/*", secondTime, secondTime, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
	require.Equal(t, "range/2", keys[0])

	// Get keys in range that includes all three
	keys, err = storage.KeysRange(testTable, "range/*", firstTime, thirdTime, 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(keys))
	// Results are ordered by created DESC
	require.Equal(t, "range/3", keys[0])
	require.Equal(t, "range/2", keys[1])
	require.Equal(t, "range/1", keys[2])

	// Test limit - should return only 2 most recent
	keys, err = storage.KeysRange(testTable, "range/*", firstTime, thirdTime, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(keys))
	require.Equal(t, "range/3", keys[0])
	require.Equal(t, "range/2", keys[1])
}

func TestMonotonicTimestamps(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create multiple entries rapidly and verify timestamps are strictly increasing
	var timestamps []int64
	for i := 0; i < 10; i++ {
		ts, err := storage.Set(testTable, "mono/"+strconv.Itoa(i), testObject)
		require.NoError(t, err)
		timestamps = append(timestamps, ts)
	}

	// Verify all timestamps are strictly increasing
	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1], "timestamp %d should be greater than %d", i, i-1)
	}

	// Verify the stored created timestamps match what was returned
	for i := 0; i < 10; i++ {
		dataList, err := storage.Get(testTable, "mono/"+strconv.Itoa(i))
		require.NoError(t, err)
		require.Equal(t, 1, len(dataList))
		require.Equal(t, timestamps[i], dataList[0].Created)
	}
}

func TestUpdateSetsUpdatedTimestamp(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create an entry
	createdTime, err := storage.Set(testTable, "upd/1", testObject)
	require.NoError(t, err)

	// Verify created timestamp and no updated timestamp
	dataList, err := storage.Get(testTable, "upd/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, createdTime, dataList[0].Created)
	require.Equal(t, int64(0), dataList[0].Updated)

	// Update the entry
	updatedTime, err := storage.Set(testTable, "upd/1", `{"updated":true}`)
	require.NoError(t, err)
	require.Greater(t, updatedTime, createdTime)

	// Verify created timestamp unchanged and updated timestamp set
	dataList, err = storage.Get(testTable, "upd/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, createdTime, dataList[0].Created)
	require.Equal(t, updatedTime, dataList[0].Updated)
}

func TestMultipleTables(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create a second table
	err := storage.CreateTable("second")
	require.NoError(t, err)

	// Set data in both tables
	_, err = storage.Set(testTable, "key/1", testObject)
	require.NoError(t, err)
	_, err = storage.Set("second", "key/1", `{"table":"second"}`)
	require.NoError(t, err)

	// Verify data is isolated between tables
	dataList, err := storage.Get(testTable, "key/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.JSONEq(t, testObject, string(dataList[0].Value))

	dataList, err = storage.Get("second", "key/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.JSONEq(t, `{"table":"second"}`, string(dataList[0].Value))

	// Keys are isolated
	keys, err := storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))

	keys, err = storage.Keys("second")
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))

	// Drop second table
	err = storage.DropTable("second")
	require.NoError(t, err)

	// Verify first table still works
	dataList, err = storage.Get(testTable, "key/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
}

func TestSetBatch(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Test batch insert
	keys := []string{"batch/1", "batch/2", "batch/3"}
	values := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}

	timestamps, err := storage.SetBatch(testTable, keys, values)
	require.NoError(t, err)
	require.Equal(t, 3, len(timestamps))

	// Verify timestamps are strictly increasing
	for i := 1; i < len(timestamps); i++ {
		require.Greater(t, timestamps[i], timestamps[i-1])
	}

	// Verify all entries were inserted
	dataList, err := storage.Get(testTable, "batch/*")
	require.NoError(t, err)
	require.Equal(t, 3, len(dataList))

	// Verify data content (ordered by created DESC)
	require.Equal(t, "batch/3", dataList[0].Key)
	require.JSONEq(t, `{"n":3}`, string(dataList[0].Value))
	require.Equal(t, "batch/2", dataList[1].Key)
	require.Equal(t, "batch/1", dataList[2].Key)

	// Verify timestamps match
	require.Equal(t, timestamps[2], dataList[0].Created) // batch/3 is most recent
	require.Equal(t, timestamps[1], dataList[1].Created)
	require.Equal(t, timestamps[0], dataList[2].Created)
}

func TestSetBatchEmpty(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Empty batch should return empty slice
	timestamps, err := storage.SetBatch(testTable, []string{}, []string{})
	require.NoError(t, err)
	require.Equal(t, 0, len(timestamps))
}

func TestSetBatchMismatchedLengths(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Mismatched lengths should return error
	_, err := storage.SetBatch(testTable, []string{"a", "b"}, []string{"1"})
	require.Error(t, err)
}

func TestSetBatchInvalidKey(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid key should return error
	_, err := storage.SetBatch(testTable, []string{"valid/1", "invalid//key"}, []string{`{}`, `{}`})
	require.Error(t, err)
}

func TestSetBatchInvalidJSON(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid JSON should return error
	_, err := storage.SetBatch(testTable, []string{"json/1"}, []string{`not json`})
	require.Error(t, err)
}

func TestSetBatchUpdate(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert initial data
	_, err := storage.Set(testTable, "updbatch/1", `{"v":1}`)
	require.NoError(t, err)

	// Batch update existing key
	timestamps, err := storage.SetBatch(testTable, []string{"updbatch/1", "updbatch/2"}, []string{`{"v":10}`, `{"v":2}`})
	require.NoError(t, err)
	require.Equal(t, 2, len(timestamps))

	// Verify update
	dataList, err := storage.Get(testTable, "updbatch/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	// JSONB normalizes whitespace, so compare JSON semantically
	require.JSONEq(t, `{"v":10}`, string(dataList[0].Value))
	// Updated entry should have updated timestamp set
	require.Greater(t, dataList[0].Updated, int64(0))
}

func TestGetByJSON(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert test data with different statuses
	_, err := storage.Set(testTable, "users/1", `{"name":"Alice","status":"active","role":"admin"}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "users/2", `{"name":"Bob","status":"inactive","role":"user"}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "users/3", `{"name":"Charlie","status":"active","role":"user"}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "orders/1", `{"status":"active","amount":100}`)
	require.NoError(t, err)

	// Query by JSON containment - find all active users
	results, err := storage.GetByJSON(testTable, "users/*", `{"status":"active"}`)
	require.NoError(t, err)
	require.Equal(t, 2, len(results))
	// Results ordered by created DESC
	require.Equal(t, "users/3", results[0].Key)
	require.Equal(t, "users/1", results[1].Key)

	// Query with multiple conditions
	results, err = storage.GetByJSON(testTable, "users/*", `{"status":"active","role":"admin"}`)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	require.Equal(t, "users/1", results[0].Key)

	// Query all tables with wildcard
	results, err = storage.GetByJSON(testTable, "*", `{"status":"active"}`)
	require.NoError(t, err)
	require.Equal(t, 3, len(results)) // 2 users + 1 order

	// Query exact key
	results, err = storage.GetByJSON(testTable, "users/1", `{"status":"active"}`)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	// Query with no matches
	results, err = storage.GetByJSON(testTable, "users/*", `{"status":"deleted"}`)
	require.NoError(t, err)
	require.Equal(t, 0, len(results))
}

func TestMultiGlob(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert test data with nested structure
	_, err := storage.Set(testTable, "stats/user1/clicks/day1", `{"count":10}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "stats/user1/clicks/day2", `{"count":20}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "stats/user2/clicks/day1", `{"count":15}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "stats/user1/views/day1", `{"count":100}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "other/data", `{"value":1}`)
	require.NoError(t, err)

	// Multi-glob: match all clicks for all users
	results, err := storage.Get(testTable, "stats/*/clicks/*")
	require.NoError(t, err)
	require.Equal(t, 3, len(results))

	// Multi-glob: match all day1 data for user1
	results, err = storage.Get(testTable, "stats/user1/*/day1")
	require.NoError(t, err)
	require.Equal(t, 2, len(results)) // clicks/day1 and views/day1

	// Multi-glob: wildcard in middle only
	results, err = storage.Get(testTable, "stats/*/clicks/day1")
	require.NoError(t, err)
	require.Equal(t, 2, len(results)) // user1 and user2

	// Single glob still works (uses optimized path)
	results, err = storage.Get(testTable, "stats/*")
	require.NoError(t, err)
	require.Equal(t, 4, len(results)) // all stats entries

	// Exact key still works
	results, err = storage.Get(testTable, "stats/user1/clicks/day1")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
}

func TestMultiGlobEdgeCases(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert test data
	_, err := storage.Set(testTable, "a/b/c/d", `{"level":4}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/x/c/y", `{"level":4}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/b/c", `{"level":3}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "a/b", `{"level":2}`)
	require.NoError(t, err)

	// Multi-glob with single wildcard in middle (not at end)
	results, err := storage.Get(testTable, "a/*/c")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	require.Equal(t, "a/b/c", results[0].Key)

	// Multi-glob matching specific depth
	results, err = storage.Get(testTable, "a/*/c/*")
	require.NoError(t, err)
	require.Equal(t, 2, len(results)) // a/b/c/d and a/x/c/y

	// Multi-glob no matches
	results, err = storage.Get(testTable, "a/*/z/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(results))

	// Multi-glob with three wildcards
	_, err = storage.Set(testTable, "x/1/y/2/z/3", `{"deep":true}`)
	require.NoError(t, err)
	results, err = storage.Get(testTable, "x/*/y/*/z/*")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	require.Equal(t, "x/1/y/2/z/3", results[0].Key)
}

func TestMultiGlobInvalidPatterns(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Double glob should fail
	_, err := storage.Get(testTable, "a/**/b")
	require.Error(t, err)

	// Double separator should fail (routed through multi-glob)
	_, err = storage.Get(testTable, "a//*/b")
	require.Error(t, err)
}

func TestIsMultiGlobFunction(t *testing.T) {
	// Test the isMultiGlob detection function directly
	tests := []struct {
		pattern  string
		expected bool
	}{
		{"users/*", false},        // single glob at end
		{"*", false},              // single glob
		{"users/1", false},        // no glob
		{"users/*/posts/*", true}, // multiple globs
		{"users/*/posts", true},   // glob in middle
		{"a/*/b/*/c", true},       // multiple globs
		{"a/*/b", true},           // glob in middle
		{"", false},               // empty
		{"no/glob/here", false},   // no glob
	}

	for _, tt := range tests {
		result := isMultiGlob(tt.pattern)
		if result != tt.expected {
			t.Errorf("isMultiGlob(%q) = %v, want %v", tt.pattern, result, tt.expected)
		}
	}
}

func TestTableStats(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert some data
	for i := 0; i < 10; i++ {
		_, err := storage.Set(testTable, "stats/"+strconv.Itoa(i), `{"n":`+strconv.Itoa(i)+`}`)
		require.NoError(t, err)
	}

	// Run ANALYZE to update statistics
	_, err := storage.Client.Exec("ANALYZE public.keys_" + testTable)
	require.NoError(t, err)

	// Get table stats
	stats, err := storage.TableStats(testTable)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.GreaterOrEqual(t, stats.RowCount, int64(10))
	require.Greater(t, stats.SizeBytes, int64(0))
}

func TestGetByField(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert test data
	_, err := storage.Set(testTable, "products/1", `{"name":"Widget","category":"electronics","price":"99.99"}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "products/2", `{"name":"Gadget","category":"electronics","price":"149.99"}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "products/3", `{"name":"Book","category":"books","price":"19.99"}`)
	require.NoError(t, err)

	// Query by field value
	results, err := storage.GetByField(testTable, "products/*", "category", "electronics")
	require.NoError(t, err)
	require.Equal(t, 2, len(results))

	// Query by different field
	results, err = storage.GetByField(testTable, "products/*", "name", "Widget")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	require.Equal(t, "products/1", results[0].Key)

	// Query all with wildcard
	results, err = storage.GetByField(testTable, "*", "category", "books")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	// Query exact key
	results, err = storage.GetByField(testTable, "products/1", "category", "electronics")
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	// Query with no matches
	results, err = storage.GetByField(testTable, "products/*", "category", "clothing")
	require.NoError(t, err)
	require.Equal(t, 0, len(results))
}

func TestStartErrors(t *testing.T) {
	// Test missing Host
	storage := &Storage{
		Name:    "postgres",
		Silence: true,
	}
	err := storage.Start()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Host and Name defined")

	// Test missing Name
	storage = &Storage{
		Host:    "localhost",
		Silence: true,
	}
	err = storage.Start()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Host and Name defined")

	// Test with custom port
	storage = &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
		Port:     "5432", // default port, should not be added to conninfo
		Silence:  true,
	}
	err = storage.Start()
	require.NoError(t, err)
	storage.Close()

	// Test with non-default port (still connects since it's the same)
	storage = &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
		Port:     "5432",
		Silence:  true,
	}
	err = storage.Start()
	require.NoError(t, err)
	storage.Close()
}

func TestStartWithCustomPoolSettings(t *testing.T) {
	storage := &Storage{
		Name:            testServerDatabase,
		User:            testServerUser,
		Host:            testServerIP,
		Password:        testServerPassword,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 1 * time.Minute,
		ConnMaxIdleTime: 30 * time.Second,
		Silence:         true,
	}
	err := storage.Start()
	require.NoError(t, err)
	defer storage.Close()

	// Verify connection works
	keys, err := storage.Keys(testTable)
	require.NoError(t, err)
	require.NotNil(t, keys)
}

func TestCreateTableError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid table name should fail
	err := storage.CreateTable("invalid-name-with-dash")
	require.Error(t, err)
}

func TestDropTableError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Dropping non-existent table should not error (IF EXISTS)
	err := storage.DropTable("nonexistent_table_xyz")
	require.NoError(t, err)
}

func TestDelError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Delete with invalid pattern should fail
	err := storage.Del(testTable, "invalid//pattern")
	require.Error(t, err)
}

func TestGetByJSONError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid JSON filter should fail
	_, err := storage.GetByJSON(testTable, "test/*", "not valid json")
	require.Error(t, err)

	// Invalid key pattern should fail
	_, err = storage.GetByJSON(testTable, "test/**", `{"valid":"json"}`)
	require.Error(t, err)
}

func TestGetByFieldError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid key pattern should fail
	_, err := storage.GetByField(testTable, "test/**", "field", "value")
	require.Error(t, err)
}

func TestTableStatsError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Non-existent table should fail
	_, err := storage.TableStats("nonexistent_table_xyz")
	require.Error(t, err)
}

func TestKeysError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Close the connection to force error
	storage.Client.Close()

	_, err := storage.Keys(testTable)
	require.Error(t, err)
}

func TestGetNWithLimit(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert multiple entries
	for i := 0; i < 5; i++ {
		_, err := storage.Set(testTable, "limit/"+strconv.Itoa(i), `{"n":`+strconv.Itoa(i)+`}`)
		require.NoError(t, err)
	}

	// GetN with limit
	results, err := storage.GetN(testTable, "limit/*", 3)
	require.NoError(t, err)
	require.Equal(t, 3, len(results))

	// GetN with limit larger than results
	results, err = storage.GetN(testTable, "limit/*", 100)
	require.NoError(t, err)
	require.Equal(t, 5, len(results))
}

func TestGetRangeFullRange(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert entries
	ts1, err := storage.Set(testTable, "range/1", `{"n":1}`)
	require.NoError(t, err)
	ts2, err := storage.Set(testTable, "range/2", `{"n":2}`)
	require.NoError(t, err)

	// GetRange with full range
	results, err := storage.GetRange(testTable, "range/*", ts1, ts2)
	require.NoError(t, err)
	require.Equal(t, 2, len(results))

	// GetRange with exact key
	results, err = storage.GetRange(testTable, "range/1", ts1, ts2)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
}

func TestClearTable(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Insert some data
	_, err := storage.Set(testTable, "clear/1", `{"n":1}`)
	require.NoError(t, err)
	_, err = storage.Set(testTable, "clear/2", `{"n":2}`)
	require.NoError(t, err)

	// Verify data exists
	keys, err := storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, 2, len(keys))

	// Clear the table
	storage.Clear(testTable)

	// Verify data is gone
	keys, err = storage.Keys(testTable)
	require.NoError(t, err)
	require.Equal(t, 0, len(keys))
}

func TestGetNError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid pattern should fail
	_, err := storage.GetN(testTable, "invalid//pattern", 10)
	require.Error(t, err)
}

func TestGetNRangeError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid pattern should fail
	_, err := storage.GetNRange(testTable, "invalid//pattern", 0, 0, 10)
	require.Error(t, err)
}

func TestGetRangeError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid pattern should fail
	_, err := storage.GetRange(testTable, "invalid//pattern", 0, 0)
	require.Error(t, err)
}

func TestKeysRangeError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid pattern should fail
	_, err := storage.KeysRange(testTable, "invalid//pattern", 0, 0, 10)
	require.Error(t, err)
}

func TestSetError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid key should fail
	_, err := storage.Set(testTable, "invalid//key", `{"n":1}`)
	require.Error(t, err)

	// Invalid JSON should fail
	_, err = storage.Set(testTable, "valid/key", `not json`)
	require.Error(t, err)
}

func TestGetError(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Invalid pattern (single glob path) should fail
	_, err := storage.Get(testTable, "invalid//pattern")
	require.Error(t, err)
}

func TestStartWithSSLMode(t *testing.T) {
	// Test with explicit SSLMode
	storage := &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
		SSLMode:  "disable",
		Silence:  true,
	}
	err := storage.Start()
	require.NoError(t, err)
	storage.Close()
}

func TestStartWithCustomMaxRetries(t *testing.T) {
	// Test with custom MaxRetries
	storage := &Storage{
		Name:       testServerDatabase,
		User:       testServerUser,
		Host:       testServerIP,
		Password:   testServerPassword,
		MaxRetries: 5,
		Silence:    true,
	}
	err := storage.Start()
	require.NoError(t, err)
	storage.Close()
}

func TestStartWithNonDefaultPort(t *testing.T) {
	// Test with non-default port - use MaxRetries=1 to fail fast
	storage := &Storage{
		Name:       testServerDatabase,
		User:       testServerUser,
		Host:       testServerIP,
		Password:   testServerPassword,
		Port:       "15432", // non-default port
		MaxRetries: 1,       // fail fast
		Silence:    true,
	}
	// This will fail since port 15432 is not listening
	err := storage.Start()
	require.Error(t, err)
}

func TestDropTableActualDrop(t *testing.T) {
	storage := newTestStorage(t)
	defer storage.Close()

	// Create a new table
	err := storage.CreateTable("temp_drop_test")
	require.NoError(t, err)

	// Insert some data
	_, err = storage.Set("temp_drop_test", "key/1", `{"n":1}`)
	require.NoError(t, err)

	// Drop the table
	err = storage.DropTable("temp_drop_test")
	require.NoError(t, err)

	// Trying to use the dropped table should fail
	_, err = storage.Get("temp_drop_test", "key/1")
	require.Error(t, err)
}
