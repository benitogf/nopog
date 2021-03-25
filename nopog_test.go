package nopog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testObject string = `{"ob":"test ✔⚓🛸🛴"}`
var testKey string = "test/"
var testServerIP string = "10.0.1.249"
var testServerDatabase string = "nopog"

func TestKeys(t *testing.T) {
	storage := &Storage{
		Name: testServerDatabase,
		IP:   testServerIP,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	keys, err := storage.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{}, keys)
	_, err = storage.Set(testKey, testObject)
	require.NoError(t, err)
	_, err = storage.Set(testKey, `not json`)
	require.Error(t, err)
	keys, err = storage.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{testKey}, keys)
}

func TestSetAndGet(t *testing.T) {
	storage := &Storage{
		Name: testServerDatabase,
		IP:   testServerIP,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	key := testKey + "1"
	_, err := storage.Set(key, testObject)
	require.NoError(t, err)
	_, err = storage.Set(key, `not json`)
	require.Error(t, err)
	dataList, err := storage.Get(key)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, key, data.Key)
	require.Equal(t, testObject, string(data.Value))
}

func TestGetPath(t *testing.T) {
	storage := &Storage{
		Name: testServerDatabase,
		IP:   testServerIP,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	_, err := storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.Get(testKey + "*")
	require.NoError(t, err)
	require.Equal(t, 2, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.Equal(t, testObject, string(data.Value))
}

func TestGetN(t *testing.T) {
	storage := &Storage{
		Name: testServerDatabase,
		IP:   testServerIP,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	_, err := storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set(testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.GetN(testKey+"*", 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.Equal(t, testObject, string(data.Value))
}

func TestRange(t *testing.T) {
	storage := &Storage{
		Name: testServerDatabase,
		IP:   testServerIP,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	_, err := storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	now := time.Now().UTC().UnixNano()
	// this sleep depends on the clock difference between the database server and your pc
	// should be 0 using ntp
	time.Sleep(time.Second * 1)
	_, err = storage.Set(testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.GetNRange(testKey+"*", now, 0, 2)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.Equal(t, testObject, string(data.Value))
	keys, err := storage.KeysRange(testKey+"*", now, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []string{testKey + "2"}, keys)
}
