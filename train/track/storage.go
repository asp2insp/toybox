package track

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/asp2insp/go-misc/utils"
)

// The track package is responsible for recording messages to a set of files.
// Each file holds CHUNK_SIZE messages, except for the active file which begins empty and grows to hold
// up to CHUNK_SIZE messages. Messages are stored in their entirety, with their wrapping.

// CHUNK_SIZE is chosen by experimentation. For small messages (~12 bytes) this was the best value
var CHUNK_SIZE uint64 = 500 * 1000

type Track struct {
	stores    []*FileStorage
	Id        string
	RootPath  string
	writeChan chan []byte
	dataCond  *sync.Cond
	alive     bool
}

func NewTrack(root, id string) *Track {
	t := Track{
		Id:       id,
		RootPath: root,
		stores:   make([]*FileStorage, 0),
		dataCond: &sync.Cond{L: &sync.Mutex{}},
		alive:    true,
	}
	t.startWriter(0)
	return &t
}

func OpenTrack(root, id string) *Track {
	t := Track{
		Id:       id,
		RootPath: root,
		stores:   make([]*FileStorage, 0),
		dataCond: &sync.Cond{L: &sync.Mutex{}},
		alive:    true,
	}
	// find and load all the stores
	for i := 0; ; i++ {
		storeId := fmt.Sprintf("%s%d", t.Id, i)
		if !exists(fname(storeId, root)) {
			break
		}
		t.stores = append(t.stores, Open(root, storeId))
	}
	var nextId uint64 = 0
	if len(t.stores) > 0 {
		nextId = uint64(len(t.stores))*CHUNK_SIZE + t.stores[len(t.stores)-1].Size
	}
	t.startWriter(nextId)
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

func (t *Track) ReaderAt(offset uint64) (io.ReadCloser, error) {
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
	close(t.writeChan) // Writer will signal alive = false
}

func (t *Track) WaitForShutdown() {
	for t.alive {
		time.Sleep(100 * time.Millisecond)
	}
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
				if chunkId > 0 {
					t.stores[chunkId-1].switchToReadOnly() // Migrate the old chunk to readonly
				}
				storeId := fmt.Sprintf("%s%d", t.Id, chunkId)
				t.stores = append(t.stores, NewFileStorage(t.RootPath, storeId, CHUNK_SIZE))
			}
			internalMsgId := int(msgId % CHUNK_SIZE)
			err := t.stores[chunkId].WriteMessage(internalMsgId, msg)
			utils.Check(err)
			msgId++

			// Tell any waiting routines that there's new data
			t.dataCond.Broadcast()
		}
	}()
}

// STORAGE READER -- Combines readers from multiple chunked files into a single interface
type StorageReader struct {
	parent     *Track
	Offset     uint64
	currentSub io.ReadCloser
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
	if err != nil {
		return 0, err
	}
	if nextMsgSize > uint64(len(p)) {
		return 0, fmt.Errorf("Message, of size %d, does not fit into available buffer", nextMsgSize)
	}
	target := p[0:nextMsgSize]
	_, err = sr.currentSub.Read(target)
	utils.Check(err)
	sr.Offset++
	sr.handleRollover()
	return int(nextMsgSize), nil
}

func (sr *StorageReader) Close() error {
	if sr.currentSub != nil {
		return sr.currentSub.Close()
	}
	return nil
}

func (sr *StorageReader) handleRollover() {
	didRollOver := sr.Offset%CHUNK_SIZE == 0
	if didRollOver {
		// We need to reset the sub reader
		if sr.Offset/CHUNK_SIZE < uint64(len(sr.parent.stores)) {
			// move to the next one
			var err error
			sr.currentSub, err = sr.parent.stores[sr.Offset/CHUNK_SIZE].ReaderAt(0)
			if err != nil {
				if sr.currentSub != nil {
					sr.currentSub.Close()
				}
				sr.currentSub = nil
			}
		} else {
			// Otherwise clear it
			sr.currentSub.Close()
			sr.currentSub = nil
		}
	}
}
