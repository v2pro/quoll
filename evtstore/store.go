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

type Config struct {
	BlockEntriesCountLimit uint16
	BlockSizeLimit         int
	MaximumFlushInterval   time.Duration
	KeepFilesCount         int
}

var defaultConfig = Config{
	BlockEntriesCountLimit: math.MaxUint16 - 1,
	BlockSizeLimit:         1024 * 1024, // byte
	MaximumFlushInterval:   1 * time.Second,
	KeepFilesCount:         24,
}

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
	Config         Config
	RootDir        string
	inputQueue     chan evtInput
	compressionBuf []byte
	currentFile    vfs.File
	currentTime    time.Time
	currentWindow  int64
}

func NewStore(rootDir string) *Store {
	return &Store{
		Config:         defaultConfig,
		RootDir:        rootDir,
		inputQueue:     make(chan evtInput, 100),
		compressionBuf: make([]byte, 1024),
	}
}

func (store *Store) Start() {
	go func() {
		for {
			store.flushInputQueue()
			store.clean()
			time.Sleep(store.Config.MaximumFlushInterval)
		}
	}()
}

func (store *Store) clean() {
	defer func() {
		recovered := recover()
		if recovered != nil {
			countlog.Fatal("event!store.clean.panic", "err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	files, err := fs.ReadDir(store.RootDir)
	if err != nil {
		countlog.Error("event!failed to read dir", "err", err, "rootDir", store.RootDir)
		return
	}
	if len(files) > store.Config.KeepFilesCount {
		for _, file := range files[:len(files)-store.Config.KeepFilesCount] {
			filePath := path.Join(store.RootDir, file.Name())
			err := fs.Remove(filePath)
			if err != nil {
				countlog.Error("event!failed to clean old file", "err", err, "filePath", filePath)
			} else {
				countlog.Info("event!cleaned_old_file", "filePath", filePath)
			}
		}
	}
}

func (store *Store) flushInputQueue() {
	defer func() {
		recovered := recover()
		if recovered != nil {
			countlog.Fatal("event!store.flushInputQueue.panic", "err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	tmpBuf := [4]byte{}
	blockBody := []byte{}
	for {
		entriesCount := uint16(0)
		blockBody = blockBody[:0]
		minCTS := uint32(math.MaxUint32)
		maxCTS := uint32(0)
		for {
			select {
			case input := <-store.inputQueue:
				if input.eventTS.Sub(store.currentTime) > time.Hour && len(blockBody) > 0 {
					err := store.saveBlock(entriesCount, minCTS, maxCTS, blockBody)
					if err != nil {
						countlog.Error("event!failed to save block", "err", err)
						return
					}
					entriesCount = uint16(0)
					blockBody = blockBody[:0]
					minCTS = uint32(math.MaxUint32)
					maxCTS = uint32(0)
				}
				if err := store.switchFile(input.eventTS); err != nil {
					countlog.Error("event!failed to switch file", "err", err)
					return
				}
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
				if entriesCount > store.Config.BlockEntriesCountLimit {
					break
				}
				if len(blockBody) > store.Config.BlockSizeLimit {
					break
				}
				continue
			default:
				if len(blockBody) > 0 {
					break
				}
				return
			}
			break
		}
		err := store.saveBlock(entriesCount, minCTS, maxCTS, blockBody)
		if err != nil {
			countlog.Error("event!failed to save block", "err", err)
			return
		}
	}
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

func (store *Store) switchFile(ts time.Time) error {
	window := ts.Unix() / 3600
	if window == store.currentWindow {
		return nil
	}
	if store.currentFile != nil {
		if err := store.currentFile.Close(); err != nil {
			return err
		}
	}
	store.currentWindow = window
	store.currentTime = time.Unix(window*3600, 0)
	fileName := store.currentTime.Format("200601021504")
	file, err := fs.OpenFile(
		path.Join(store.RootDir, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		file, err = fs.OpenFile(
			path.Join(store.RootDir, fileName), os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
	} else {
		header := []byte{0xD1, 0xD1, 1, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(header[3:7], uint32(store.currentTime.Unix()))
		_, err = file.Write(header)
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
		eventTS:   timeutil.Now(),
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
