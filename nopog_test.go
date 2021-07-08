package nopog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testObject string = `{"ob":"test ✔⚓🛸🛴"}`
var testKey string = "test/"

var testServerIP string = "localhost"
var testServerDatabase string = "postgres"
var testServerUser = "postgres"
var testServerPassword = "postgres"

func TestKeys(t *testing.T) {
	storage := &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	keys, err := storage.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{}, keys)
	_, err = storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	_, err = storage.Set("notValidKey//", testObject)
	require.Error(t, err)
	_, err = storage.Set("notValidKey/**", testObject)
	require.Error(t, err)
	_, err = storage.Set("notValidKey/*/", testObject)
	require.Error(t, err)
	_, err = storage.Set(testKey+"1", `not json`)
	require.Error(t, err)
	keys, err = storage.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{testKey + "1"}, keys)
}

func TestSetAndGet(t *testing.T) {
	storage := &Storage{
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
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
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
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
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
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
		Name:     testServerDatabase,
		User:     testServerUser,
		Host:     testServerIP,
		Password: testServerPassword,
	}
	storage.Start()
	defer storage.Close()
	storage.Clear()
	_, err := storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	secondOpTime, err := storage.Set(testKey+"2", testObject)
	require.NoError(t, err)
	dataList, err := storage.GetNRange(testKey+"*", secondOpTime, secondOpTime, 2)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data := dataList[0]
	require.NoError(t, err)
	require.Equal(t, testKey+"2", data.Key)
	require.Equal(t, testObject, string(data.Value))
	keys, err := storage.KeysRange(testKey+"*", secondOpTime, secondOpTime, 2)
	require.NoError(t, err)
	require.Equal(t, []string{testKey + "2"}, keys)

	dataList, err = storage.GetRange(testKey+"*", secondOpTime, secondOpTime)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data = dataList[0]
	require.Equal(t, testKey+"2", data.Key)
	require.Equal(t, testObject, string(data.Value))
	keys, err = storage.KeysRange(testKey+"*", secondOpTime, secondOpTime, 2)
	require.NoError(t, err)
	require.Equal(t, []string{testKey + "2"}, keys)

	dataList, err = storage.GetUpdatedRange(testKey+"*", secondOpTime, secondOpTime)
	require.NoError(t, err)
	require.Equal(t, 0, len(dataList))

	thirdOpTime, err := storage.Set(testKey+"1", testObject)
	require.NoError(t, err)
	dataList, err = storage.GetUpdatedRange(testKey+"*", secondOpTime, thirdOpTime)
	require.NoError(t, err)
	require.Equal(t, 1, len(dataList))
	data = dataList[0]
	require.Equal(t, testKey+"1", data.Key)

}
