package evtstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/blang/vfs/memfs"
	"time"
	"github.com/v2pro/quoll/timeutil"
)

func init() {
	epoch := time.Unix(1483228900, 0)
	timeutil.MockNow(epoch)
}

func reset() {
	fs = memfs.Create()
	fs.Mkdir("/tmp", 0666)
}

func Test_add_one(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	err := testStore.Add([]byte(`{"url":"/hello"}`))
	should.Nil(err)
	testStore.flushInputQueue()
	dir, _ := fs.ReadDir("/tmp")
	should.Len(dir, 1)
	should.Equal("201701010800", dir[0].Name())
}

func Test_add_multiple(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	err := testStore.Add([]byte(`{"url":"/hello"}`))
	should.Nil(err)
	err = testStore.Add([]byte(`{"url":"/hello"}`))
	should.Nil(err)
	testStore.flushInputQueue()
	dir, _ := fs.ReadDir("/tmp")
	should.Len(dir, 1)
	should.Equal("201701010800", dir[0].Name())
}

func Test_list(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	events, err := testStore.List(timeutil.Now(), 0, 2)
	should.Nil(err)
	should.Len(events, 2)
}
