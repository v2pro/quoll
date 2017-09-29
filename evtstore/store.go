package evtstore

import (
	"os"
	"github.com/blang/vfs"
	"path"
	"time"
	"io"
	"encoding/binary"
)

type EventJson []byte

var fs vfs.Filesystem = vfs.OS()
var mockedNow *time.Time

type Store struct {
	RootDir string
}

func (store *Store) Add(eventJson EventJson) error {
	ts := now().Format("200601021504")
	file, err := fs.OpenFile(
		path.Join(store.RootDir, ts), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		file, err = fs.OpenFile(
			path.Join(store.RootDir, ts), os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
	}
	defer file.Close()
	file.Seek(0, io.SeekEnd)
	var lenBytes [4]byte
	binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(eventJson)))
	_, err = file.Write(lenBytes[:])
	if err != nil {
		return err
	}
	_, err = file.Write(eventJson)
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) List(targetTime time.Time, from int, size int) ([]EventJson, error) {
	ts := now().Format("200601021504")
	file, err := fs.OpenFile(
		path.Join(store.RootDir, ts), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	var lenBytes [4]byte
	var events []EventJson
	for i := 0; i < size; i++ {
		_, err = io.ReadFull(file, lenBytes[:])
		if err != nil {
			return nil, err
		}
		eventLen := binary.LittleEndian.Uint32(lenBytes[:])
		event := make([]byte, eventLen)
		_, err = io.ReadFull(file, event)
		if err != nil {
			return nil, err
		}
		if i >= from {
			events = append(events, event)
		}
	}
	return events, nil
}

func now() time.Time {
	if mockedNow != nil {
		return *mockedNow
	}
	return time.Now()
}
