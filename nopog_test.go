package nopog

import (
	"strconv"
	"testing"

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
	}
	err := storage.Start()
	require.NoError(t, err)
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
	require.Equal(t, testObject, string(data.Value))
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
	require.Equal(t, testObject, string(data.Value))
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
	require.Equal(t, testObject, string(data.Value))
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
	require.Equal(t, testObject, string(dataList[0].Value))
	require.Equal(t, testKey+"1", dataList[1].Key)
	require.Equal(t, testObject, string(dataList[1].Value))

	// Get only the second entry using its timestamp as lower bound
	dataList, err = storage.GetNRange(testTable, testKey+"*", secondOpTime, 0, 2)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, testKey+"2", dataList[0].Key)
	require.Equal(t, testObject, string(dataList[0].Value))

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

	// Invalid: multiple globs
	_, err := storage.Get(testTable, "items/*/sub/*")
	require.Error(t, err)

	// Invalid: glob in middle of path
	_, err = storage.Get(testTable, "items/*/sub")
	require.Error(t, err)

	// Invalid: double glob
	_, err = storage.Get(testTable, "items/**")
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
	require.Equal(t, testObject, string(dataList[0].Value))
	require.Equal(t, "limit/2", dataList[1].Key)
	require.Equal(t, testObject, string(dataList[1].Value))

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
	require.Equal(t, testObject, string(dataList[0].Value))

	dataList, err = storage.Get("second", "key/1")
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	require.Equal(t, `{"table":"second"}`, string(dataList[0].Value))

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
	require.Equal(t, `{"n":3}`, string(dataList[0].Value))
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
	require.Equal(t, `{"v":10}`, string(dataList[0].Value))
	// Updated entry should have updated timestamp set
	require.Greater(t, dataList[0].Updated, int64(0))
}
