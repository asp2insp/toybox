package track

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"unsafe"

	"github.com/asp2insp/go-misc/utils"
	"github.com/edsrzf/mmap-go"
)

// A file storage blob represents a fixed-count array of untyped, unsized blobs on disk.
// The size of the array must be specified at time of creation,
// For fast access, the first 4 bytes stores the length, and then the following 8 * (length + 1)
// bytes will be an offset table where each entry's offset is inserted as it is written.
// Example: a FileStorage with capacity for 100 messages which currently has 1 message of size
// 40 bytes inserted will have the following structure:
// Byte Range: Contents
//      [0-3]: 100
//     [4-11]: 812        // Offset of the first message is the first byte address after the index
//    [12-19]: 852        // Next message will begin after first message ends
//   [20-811]: 0          // Remainder of the index is empty. Index length is 101 uint32s since we store
//                        // beginning and end offsets for each message
//  [812-851]: MESSAGE1
//  Remainder of the file is empty
//
//
// NOTE: THIS CLASS IS NOT THREAD-SAFE. Atomicity of operations must be implemented by
// client code! Recommended use is to have a single goroutine manage access to a given FileStorage
// instance.

type FileStorage struct {
	fileId       string
	rootPath     string
	file         *os.File
	Capacity     uint64
	Size         uint32
	headerMemory mmap.MMap
	fileMemory   mmap.MMap
	index        []uint64
}

const _nSize = 8 // sizeof(uint64)

func NewFileStorage(root, id string, capacity uint64) *FileStorage {
	f := FileStorage{
		rootPath: root,
		Capacity: capacity,
		Size:     0,
	}
	return f.init(id)
}

// STORAGE
func (store *FileStorage) init(id string) *FileStorage {
	store.fileId = id

	// Init the header
	headerSize := (store.Capacity + 2) * _nSize // Size of array + offset table in bytes
	store.file = open(fname(store.fileId, store.rootPath), os.O_RDWR|os.O_CREATE)
	var err error
	store.headerMemory, err = mmap.MapRegion(store.file, int(headerSize), mmap.RDWR, 0, 0)
	utils.Check(err)
	index := mmapToIndex(store.headerMemory, 0, headerSize)
	index[0] = store.Capacity
	store.index = index[1:]
	store.index[0] = headerSize
	_, err = store.file.Seek(int64(headerSize), os.SEEK_SET)
	utils.Check(err)
	return store
}

// Write the given message to the storage.
func (store *FileStorage) WriteMessage(index int, data []byte) error {
	if uint32(index) != store.Size {
		return fmt.Errorf("Out of order message. Expected %d but got %d", store.Size, index)
	} else if index < 0 || uint64(index) >= store.Capacity {
		return fmt.Errorf("Index %d out of bounds [0, %d]", index, store.Capacity)
	}
	_, err := store.file.Write(data)
	if err != nil {
		return err
	}
	store.Size++
	store.index[index+1] = store.index[index] + uint64(len(data))
	return nil
}

// Return a reader pointing to the beginning of the message with the given index
func (store *FileStorage) ReaderAt(messageIndex int) (io.Reader, error) {
	if uint32(messageIndex) >= store.Size {
		return nil, fmt.Errorf("Index %d exceeds available size of %d", messageIndex, store.Size)
	} else if messageIndex < 0 || uint64(messageIndex) >= store.Capacity {
		return nil, fmt.Errorf("Index %d out of bounds [0, %d]", messageIndex, store.Capacity)
	}

	r, err := os.Open(store.file.Name())
	if err != nil {
		return nil, err
	}
	n, err := r.Seek(int64(store.index[messageIndex]), os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Return the size in bytes of the message at the given index
func (store *FileStorage) SizeOf(messageIndex int) (uint64, error) {
	if uint32(messageIndex) >= store.Size {
		return 0, fmt.Errorf("Index %d exceeds available size of %d", messageIndex, store.Size)
	} else if messageIndex < 0 || uint64(messageIndex) >= store.Capacity {
		return 0, fmt.Errorf("Index %d out of bounds [0, %d]", messageIndex, store.Capacity)
	}
	return store.index[messageIndex+1] - store.index[messageIndex], nil
}

// Flush any pending writes to disk
func (store *FileStorage) Flush() {
	store.file.Sync()
	store.headerMemory.Flush()
}

// CLOSABLE

// Close this storage, by closing the file
// pointers and unmapping all memory
func (store *FileStorage) Close() {
	store.headerMemory.Flush()
	store.headerMemory.Unmap()
	store.file.Close()
}

// UTILS

// Open the given file with the given flags
func open(path string, fileFlags int) *os.File {
	file, err := os.OpenFile(path, fileFlags, 0666)
	utils.Check(err)
	if utils.Filesize(file) == 0 {
		err = file.Truncate(int64(os.Getpagesize()))
		utils.Check(err)
	}
	return file
}

// Return a path to the file named with the given id.
// If a root dir is provided, the file will be relative
// to that root. Otherwise it is placed in the tmpdir
func fname(id, root string) string {
	if root != "" {
		return filepath.Join(root, id)
	} else {
		return filepath.Join(os.TempDir(), id)
	}
}

// Cast the []byte represented by the mmapped region
// to an array of integers
func mmapToIndex(data mmap.MMap, offset, size uint64) []uint64 {
	d := data[offset : offset+size]
	dHeader := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	dHeader.Len /= _nSize
	dHeader.Cap /= _nSize
	return *(*[]uint64)(unsafe.Pointer(dHeader))
}
