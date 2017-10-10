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

func Test_rotation_happen_between_flush(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	testStore.flushInputQueue()
	timeutil.MockNow(timeutil.Now().Add(time.Hour))
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	testStore.flushInputQueue()
	dir, _ := fs.ReadDir("/tmp")
	should.Len(dir, 2)
	should.Equal("201701010800", dir[0].Name())
	should.Equal("201701010900", dir[1].Name())
}

func Test_rotation_happen_within_flush(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	timeutil.MockNow(timeutil.Now().Add(time.Hour))
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	testStore.flushInputQueue()
	dir, _ := fs.ReadDir("/tmp")
	should.Len(dir, 2)
	should.Equal("201701010800", dir[0].Name())
	should.True(dir[0].Size() > 0)
	should.Equal("201701010900", dir[1].Name())
	should.True(dir[1].Size() > 0)
}

func Test_clean(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	testStore.Config.KeepFilesCount = 1
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	testStore.flushInputQueue()
	timeutil.MockNow(timeutil.Now().Add(time.Hour))
	should.Nil(testStore.Add([]byte(`{"url":"/hello"}`)))
	testStore.flushInputQueue()
	testStore.clean()
	dir, _ := fs.ReadDir("/tmp")
	should.Len(dir, 1)
	should.Equal("201701010900", dir[0].Name())
}

func Test_list(t *testing.T) {
	reset()
	should := require.New(t)
	var testStore = NewStore("/tmp")
	should.Nil(testStore.Add([]byte(`{"url":"/hello1"}`)))
	should.Nil(testStore.Add([]byte(`{"url":"/hello2"}`)))
	testStore.flushInputQueue()
	events, err := testStore.List(timeutil.Now(), timeutil.Now().Add(time.Hour * 24), 0, 2)
	should.Nil(err)
	blockId, block, events := events.Next()
	should.Equal("201701010800", blockId.FileName())
	should.Equal(uint32(0), blockId.IndexWithinFile())
	entries := block.EventEntries()
	entry, entries := entries.Next()
	should.Equal(`{"url":"/hello1"}`, string(entry.EventBody()))
	entry, entries = entries.Next()
	should.Equal(`{"url":"/hello2"}`, string(entry.EventBody()))
}
