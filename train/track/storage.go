package track

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/asp2insp/go-misc/utils"
)

// The track package is responsible for recording messages to a set of files.
// Each file holds CHUNK_SIZE messages, except for the active file which begins empty and grows to hold
// up to CHUNK_SIZE messages. Messages are stored in their entirety, with their wrapping.

var CHUNK_SIZE uint64 = 1000

type Track struct {
	stores    []*FileStorage
	Id        string
	RootPath  string
	writeChan chan []byte
	dataCond  *sync.Cond
	alive     bool
	metadata  *meta
}

type meta struct {
	NextId uint64
}

func NewTrack(root, id string) *Track {
	t := Track{
		Id:       id,
		RootPath: root,
		stores:   make([]*FileStorage, 0),
		dataCond: &sync.Cond{L: &sync.Mutex{}},
		alive:    true,
		metadata: &meta{NextId: 0},
	}
	t.startWriter(0)
	return &t
}

func OpenTrack(root, id string) *Track {
	metaPath := fname(id+"_meta", root)
	metaFile := open(metaPath, os.O_RDONLY)
	defer metaFile.Close()
	decoder := gob.NewDecoder(metaFile)
	m := &meta{}
	err := decoder.Decode(m)
	if err != nil {
		if err.Error() == "EOF" {
			m.NextId = 0
		} else {
			utils.Check(err)
		}
	}
	numStores := 0
	if m.NextId != 0 {
		numStores = int(math.Ceil(float64(m.NextId) / float64(CHUNK_SIZE)))
	}

	t := Track{
		Id:       id,
		RootPath: root,
		stores:   make([]*FileStorage, numStores),
		dataCond: &sync.Cond{L: &sync.Mutex{}},
		alive:    true,
		metadata: m,
	}
	for i := 0; i < numStores; i++ {
		storeId := fmt.Sprintf("%s%d", t.Id, i)
		t.stores[i] = Open(root, storeId)
	}
	t.startWriter(m.NextId)
	return &t
}

func (t *Track) WriteMessage(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Track is closed, could not write message")
		}
	}()
	t.writeChan <- data
	return nil
}

func (t *Track) ReaderAt(offset uint64) (io.Reader, error) {
	if offset < 0 {
		return nil, fmt.Errorf("Offset out of bounds: %d", offset)
	}
	r := &StorageReader{
		parent: t,
		Offset: offset,
		mutex:  &sync.Mutex{},
	}
	chunkIndex := offset / CHUNK_SIZE
	msgIndex := offset % CHUNK_SIZE
	if chunkIndex < uint64(len(t.stores)) && uint64(msgIndex) < t.stores[chunkIndex].Size {
		var err error
		r.currentSub, err = t.stores[chunkIndex].ReaderAt(msgIndex)
		utils.Check(err)
	}
	return r, nil
}

func (t *Track) Close() {
	// TODO handle updating this in failure case, maybe FS scan?
	metaPath := fname(t.Id+"_meta", t.RootPath)
	metaFile := open(metaPath, os.O_RDWR|os.O_CREATE)
	defer metaFile.Close()
	metaFile.Truncate(0)
	encoder := gob.NewEncoder(metaFile)
	err := encoder.Encode(t.metadata)
	utils.Check(err)
	close(t.writeChan) // Writer will signal alive = false
}

func (t *Track) startWriter(startId uint64) {
	t.writeChan = make(chan []byte, CHUNK_SIZE/100) // Buffer 1% of a chunk
	go func() {
		msgId := startId
		for {
			msg, more := <-t.writeChan
			if !more {
				t.alive = false
				return
			}
			chunkId := msgId / CHUNK_SIZE
			if chunkId == uint64(len(t.stores)) {
				storeId := fmt.Sprintf("%s%d", t.Id, chunkId)
				t.stores = append(t.stores, NewFileStorage(t.RootPath, storeId, CHUNK_SIZE))
			}
			internalMsgId := int(msgId % CHUNK_SIZE)
			err := t.stores[chunkId].WriteMessage(internalMsgId, msg)
			utils.Check(err)
			msgId++
			t.metadata.NextId = msgId + 1

			// Tell any waiting routines that there's new data
			t.dataCond.Broadcast()
		}
	}()
}

// STORAGE READER -- Combines readers from multiple chunked files into a single interface
type StorageReader struct {
	parent     *Track
	Offset     uint64
	currentSub io.Reader
	mutex      *sync.Mutex
}

// Read is thread-safe
func (sr *StorageReader) Read(p []byte) (n int, err error) {
	sr.mutex.Lock()
	defer sr.mutex.Unlock()

	if !sr.parent.alive {
		return -1, errors.New("EOF")
	}

	chunkId := sr.Offset / CHUNK_SIZE
	internalMsgId := uint64(sr.Offset % CHUNK_SIZE)

	sr.parent.dataCond.L.Lock()
	for sr.currentSub == nil ||
		chunkId >= uint64(len(sr.parent.stores)) ||
		internalMsgId >= sr.parent.stores[chunkId].Size {
		// Block for new data
		sr.parent.dataCond.Wait()
		sr.handleRollover()
	}
	sr.parent.dataCond.L.Unlock()
	// We have a valid reader, and can read from it
	nextMsgSize, err := sr.parent.stores[chunkId].SizeOf(internalMsgId)
	utils.Check(err)
	target := p[0:nextMsgSize]
	_, err = sr.currentSub.Read(target)
	utils.Check(err)
	sr.Offset++
	sr.handleRollover()
	return int(nextMsgSize), nil
}

func (sr *StorageReader) handleRollover() {
	didRollOver := sr.Offset%CHUNK_SIZE == 0
	if didRollOver {
		// We need to reset the sub reader
		if sr.Offset/CHUNK_SIZE < uint64(len(sr.parent.stores)) {
			// move to the next one
			var err error
			sr.currentSub, err = sr.parent.stores[sr.Offset/CHUNK_SIZE].ReaderAt(0)
			utils.Check(err)
		} else {
			// Otherwise clear it
			sr.currentSub = nil
		}
	}
}
