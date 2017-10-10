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
	"bytes"
)

const fileHeaderSize = 7
const blockHeaderSize = 18
const blockIdSize = 16
const entryHeaderSize = 8

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

type EventEntry []byte // size(4byte)|timestamp(4byte)|body

func (entry EventEntry) EventCTS() uint32 {
	return binary.LittleEndian.Uint32(entry[4:])
}
func (entry EventEntry) EventBody() EventBody {
	return EventBody(entry[8:])
}

type CompressedEventEntries []byte
type EventEntries []byte

func (entries EventEntries) Next() (EventEntry, EventEntries) {
	if len(entries) < entryHeaderSize {
		panic("no more entry")
	}
	size := binary.LittleEndian.Uint32(entries)
	return EventEntry(entries[:size+entryHeaderSize]), entries[size+entryHeaderSize:]
}

type EventBlocks []byte // EventBlockId|EventBlock|EventBlockId|EventBlock|...

func (blocks EventBlocks) Next() (EventBlockId, EventBlock, EventBlocks) {
	if len(blocks) < blockIdSize {
		panic("no more block")
	}
	blockId := EventBlockId(blocks[:blockIdSize])
	blockHeader := EventBlock(blocks[blockIdSize:blockIdSize+blockHeaderSize])
	next := blockIdSize + blockHeaderSize + blockHeader.CompressedSize()
	block := EventBlock(blocks[blockIdSize:next])
	return blockId, block, blocks[next:]
}

type EventBlockId []byte // filename(12byte)|indexWithinFile(4byte)

func (blockId EventBlockId) FileName() string {
	return string(blockId[:12])
}

func (blockId EventBlockId) IndexWithinFile() uint32 {
	return binary.LittleEndian.Uint32(blockId[12:])
}

type EventBlock []byte // compressedSize(4byte)|uncompressedSize(4byte)|count(2byte)|minTimestamp(4byte)|maxTimestamp(4byte)|body

func (blk EventBlock) CompressedSize() uint32 {
	return binary.LittleEndian.Uint32(blk)
}
func (blk EventBlock) UncompressedSize() uint32 {
	return binary.LittleEndian.Uint32(blk[4:])
}
func (blk EventBlock) EntriesCount() uint16 {
	return binary.LittleEndian.Uint16(blk[8:])
}
func (blk EventBlock) MinCTS() uint32 {
	return binary.LittleEndian.Uint32(blk[10:])
}
func (blk EventBlock) MaxCTS() uint32 {
	return binary.LittleEndian.Uint32(blk[14:])
}
func (blk EventBlock) CompressedEventEntries() CompressedEventEntries {
	return CompressedEventEntries(blk[18:])
}
func (blk EventBlock) EventEntries() EventEntries {
	entries := make([]byte, blk.UncompressedSize())
	lz4.DecompressSafe(blk.CompressedEventEntries(), entries)
	return EventEntries(entries)
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
	var blockHeader [blockHeaderSize]byte
	bound := lz4.CompressBound(len(blockBody))
	if len(store.compressionBuf) < bound {
		store.compressionBuf = make([]byte, bound)
	}
	compressedSize := lz4.CompressDefault(blockBody, store.compressionBuf)
	binary.LittleEndian.PutUint32(blockHeader[0:4], uint32(compressedSize))
	binary.LittleEndian.PutUint32(blockHeader[4:8], uint32(len(blockBody)))
	binary.LittleEndian.PutUint16(blockHeader[8:10], uint16(entriesCount))
	binary.LittleEndian.PutUint32(blockHeader[10:14], minCTS)
	binary.LittleEndian.PutUint32(blockHeader[14:blockHeaderSize], maxCTS)
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
		header := [fileHeaderSize]byte{0xD1, 0xD1, 1, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(header[3:7], uint32(store.currentTime.Unix()))
		_, err = file.Write(header[:])
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

func (store *Store) List(startTime time.Time, endTime time.Time, skip int, limit int) (EventBlocks, error) {
	files, err := fs.ReadDir(store.RootDir)
	if err != nil {
		return nil, err
	}
	eventBlocks := bytes.NewBuffer(nil)
	var headerBuf = [blockHeaderSize]byte{}
	var header EventBlock = headerBuf[:]
	readEntriesCount := 0
	for _, fileInfo := range files {
		blockIdTmpl := []byte(fileInfo.Name())
		blockIdTmpl = append(blockIdTmpl, []byte{0, 0, 0, 0}...)
		file, err := fs.OpenFile(path.Join(store.RootDir, fileInfo.Name()), os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		file.Seek(fileHeaderSize, io.SeekStart)
		index := 0
		for {
			_, err = io.ReadFull(file, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint32(blockIdTmpl[12:], uint32(index))
			_, err = eventBlocks.Write(blockIdTmpl)
			if err != nil {
				return nil, err
			}
			_, err = eventBlocks.Write(header)
			if err != nil {
				return nil, err
			}
			_, err = io.CopyN(eventBlocks, file, int64(header.CompressedSize()))
			if err != nil {
				return nil, err
			}
			readEntriesCount += int(header.EntriesCount())
			if readEntriesCount > limit {
				return EventBlocks(eventBlocks.Bytes()), nil
			}
			index++
		}
	}
	return EventBlocks(eventBlocks.Bytes()), nil
}
