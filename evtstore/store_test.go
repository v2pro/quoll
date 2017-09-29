package evtstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/blang/vfs/memfs"
	"time"
)


func init() {
	epoch := time.Unix(0, 0)
	mockedNow = &epoch
}

func reset() {
	fs = memfs.Create()
	fs.Mkdir("/tmp", 0666)
}

func Test_add(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = &Store{
		RootDir: "/tmp",
	}
	err := testStore.Add([]byte(`{"url":"/hello"}`))
	should.Nil(err)
	dir, _ := fs.ReadDir("/tmp")
	should.Equal("197001010800", dir[0].Name())
}

func Test_list(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = &Store{
		RootDir: "/tmp",
	}
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	events, err := testStore.List(now(), 0, 2)
	should.Nil(err)
	should.Len(events, 2)
}
