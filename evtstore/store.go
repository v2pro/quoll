package evtstore

import (
	"os"
	"github.com/blang/vfs"
	"path"
	"time"
	"io"
	"encoding/binary"
	"github.com/v2pro/quoll/lz4"
)

type EventJson []byte

type CompressedEvent struct {
	OriginalSize uint32
	Data         []byte
}

var fs vfs.Filesystem = vfs.OS()
var mockedNow *time.Time

type Store struct {
	RootDir        string
	compressionBuf []byte
	currentFile vfs.File
}

func (store *Store) openFile() error {
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
	file.Seek(0, io.SeekEnd)
	store.currentFile = file
	return nil
}

func (store *Store) Add(eventJson EventJson) error {
	if store.currentFile == nil {
		if err := store.openFile(); err != nil {
			return err
		}
	}
	file := store.currentFile
	var lenBytes [4]byte
	bound := lz4.CompressBound(len(eventJson))
	if len(store.compressionBuf) < bound {
		store.compressionBuf = make([]byte, bound)
	}
	compressedSize := lz4.CompressDefault(eventJson, store.compressionBuf)
	binary.LittleEndian.PutUint32(lenBytes[:], uint32(compressedSize+4))
	_, err := file.Write(lenBytes[:])
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(eventJson)))
	_, err = file.Write(lenBytes[:])
	if err != nil {
		return err
	}
	_, err = file.Write(store.compressionBuf[:compressedSize])
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) List(targetTime time.Time, from int, size int) ([]CompressedEvent, error) {
	ts := now().Format("200601021504")
	file, err := fs.OpenFile(
		path.Join(store.RootDir, ts), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	var lenBytes [4]byte
	var events []CompressedEvent
	for i := 0; i < size; i++ {
		_, err = io.ReadFull(file, lenBytes[:])
		if err != nil {
			return nil, err
		}
		compressedEventSize := binary.LittleEndian.Uint32(lenBytes[:])
		compressedEvent := make([]byte, compressedEventSize)
		_, err = io.ReadFull(file, compressedEvent)
		if err != nil {
			return nil, err
		}
		if i >= from {
			events = append(events, CompressedEvent{
				OriginalSize: binary.LittleEndian.Uint32(compressedEvent),
				Data:         compressedEvent[4:],
			})
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
