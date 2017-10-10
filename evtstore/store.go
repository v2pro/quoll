package evtstore

import (
	"os"
	"github.com/blang/vfs"
	"path"
	"time"
	"io"
	"encoding/binary"
	"github.com/v2pro/plz/countlog"
	"errors"
	"math"
	"github.com/v2pro/quoll/lz4"
	"github.com/v2pro/quoll/timeutil"
)

const blockEntriesCountLimit = math.MaxUint16 - 1
const blockSizeLimit = 1024 * 1024 // byte
const maximumFlushInterval = 1 * time.Second

type EventBody []byte
type evtInput struct {
	eventTS   time.Time
	eventBody EventBody
}
type evtEntry []byte // size(4byte)|timestamp(4byte)|body

type evtBlock []byte // compressedSize(4byte)|uncompressedSize(4byte)|count(2byte)|minTimestamp(4byte)|maxTimestamp(4byte)|body

type CompressedEvent struct {
	OriginalSize uint32
	Data         []byte
}

var fs vfs.Filesystem = vfs.OS()

type Store struct {
	RootDir        string
	inputQueue     chan evtInput
	compressionBuf []byte
	currentFile    vfs.File
	currentTime    time.Time
}

func NewStore(rootDir string) *Store {
	return &Store{
		RootDir:        rootDir,
		inputQueue:     make(chan evtInput, 100),
		compressionBuf: make([]byte, 1024),
	}
}

func (store *Store) Start() {
	go func() {
		for {
			store.flushInputQueue()
			time.Sleep(maximumFlushInterval)
		}
	}()
}

func (store *Store) flushInputQueue() error {
	defer func() {
		recovered := recover()
		if recovered != nil {
			countlog.Fatal("event!store.flushInputQueue.panic", "err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	if store.currentFile == nil {
		if err := store.openFile(); err != nil {
			return err
		}
	}
	tmpBuf := [4]byte{}
	for {
		entriesCount := uint16(0)
		blockBody := []byte{}
		minCTS := uint32(math.MaxUint32)
		maxCTS := uint32(0)
		for {
			select {
			case input := <-store.inputQueue:
				eventCTS := timeutil.Compress(store.currentTime, input.eventTS)
				if eventCTS > maxCTS {
					maxCTS = eventCTS
				}
				if eventCTS < minCTS {
					minCTS = eventCTS
				}
				entriesCount++
				binary.LittleEndian.PutUint32(tmpBuf[:], uint32(len(input.eventBody)))
				blockBody = append(blockBody, tmpBuf[:]...)
				binary.LittleEndian.PutUint32(tmpBuf[:], eventCTS)
				blockBody = append(blockBody, tmpBuf[:]...)
				blockBody = append(blockBody, input.eventBody...)
				if entriesCount > blockEntriesCountLimit {
					break
				}
				if len(blockBody) > blockSizeLimit {
					break
				}
				continue
			default:
				if len(blockBody) > 0 {
					break
				}
				return nil
			}
			break
		}
		err := store.saveBlock(entriesCount, minCTS, maxCTS, blockBody)
		if err != nil {
			countlog.Error("event!failed to save block", "err", err)
			return err
		}
	}
	return nil
}

func (store *Store) saveBlock(entriesCount uint16, minCTS, maxCTS uint32, blockBody []byte) error {

	file := store.currentFile
	var blockHeader [18]byte
	bound := lz4.CompressBound(len(blockBody))
	if len(store.compressionBuf) < bound {
		store.compressionBuf = make([]byte, bound)
	}
	compressedSize := lz4.CompressDefault(blockBody, store.compressionBuf)
	binary.LittleEndian.PutUint32(blockHeader[0:4], uint32(compressedSize+len(blockHeader)))
	binary.LittleEndian.PutUint32(blockHeader[4:8], uint32(len(blockBody)))
	binary.LittleEndian.PutUint16(blockHeader[8:10], uint16(entriesCount))
	binary.LittleEndian.PutUint32(blockHeader[10:14], minCTS)
	binary.LittleEndian.PutUint32(blockHeader[14:18], maxCTS)
	_, err := file.Write(blockHeader[:])
	if err != nil {
		return err
	}
	_, err = file.Write(store.compressionBuf[:compressedSize])
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) openFile() error {
	now := timeutil.Now()
	store.currentTime = time.Unix((now.Unix() / 3600) * 3600, 0)
	ts := store.currentTime.Format("200601021504")
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

func (store *Store) Add(eventBody EventBody) error {
	select {
	case store.inputQueue <- evtInput{
		eventBody: eventBody,
		eventTS:  timeutil.Now(),
	}:
		return nil
	default:
		return errors.New("input queue overflow")
	}
}

func (store *Store) List(targetTime time.Time, from int, size int) ([]CompressedEvent, error) {
	ts := timeutil.Now().Format("200601021504")
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
