# nopog

[![Test](https://github.com/benitogf/nopog/actions/workflows/tests.yml/badge.svg)](https://github.com/benitogf/nopog/actions/workflows/tests.yml)

key value abstraction using postgres json column type as value

![Alt text](erd.PNG?raw=true "ERD")

## interface

```
type Object struct {
	Created int64           `json:"created"`
	Updated int64           `json:"updated"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
}

Start()
Close()
Clear()
Keys() ([]string, error)
KeysRange(path string, from, to int64, limit int) ([]string, error)
Get(path string) ([]Object, error)
GetN(path string, limit int) ([]Object, error)
GetNRange(path string, from, to int64, limit int) ([]Object, error)
Set(key string, value string) (string, error)
Del(path string) error
```

# quickstart

Create a database in your postgresql server and run the [sql script](nopog.sql)

```
go get github.com/benitogf/nopog
```

use in your application as:

```
storage := &nopog.Storage{
    Name: "nopog",
    IP:   "10.0.1.249",
}
storage.Start()
_, err := storage.Set("test/1", `{"ob":"test ✔⚓🛸🛴"}`)
dataList, err := storage.Get(key)
log.Println(dataList[0])
```
