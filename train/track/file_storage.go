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
// For fast access, the first 8 bytes stores the length, and then the following 8 * (length + 1)
// bytes will be an offset table where each entry's offset is inserted as it is written.
// Example: a FileStorage with capacity for 100 messages which currently has 1 message of size
// 40 bytes inserted will have the following structure:
// Byte Range: Contents
//      [0-7]: 100
//     [8-15]: 816        // Offset of the first message is the first byte address after the index
//    [16-23]: 856        // Next message will begin after first message ends
//   [24-815]: 0          // Remainder of the index is empty. Index length is 101 uint32s since we store
//                        // beginning and end offsets for each message
//  [816-855]: MESSAGE1
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
	Size         uint64
	headerMemory mmap.MMap
	fileMemory   mmap.MMap
	index        []uint64
}

const _nSize = 8 // sizeof(uint64)

// Create the file storage with the given path and name
func NewFileStorage(root, id string, capacity uint64) *FileStorage {
	f := FileStorage{
		fileId:   id,
		rootPath: root,
		Capacity: capacity,
		Size:     0,
	}
	return f.init()
}

// Open the file storage with the given path and name
func Open(root, id string) *FileStorage {
	store := FileStorage{
		fileId:   id,
		rootPath: root,
	}
	store.file = open(fname(store.fileId, store.rootPath), os.O_RDWR)
	// Find the header size
	var err error
	capMem, err := mmap.MapRegion(store.file, _nSize, mmap.RDWR, 0, 0)
	utils.Check(err)
	capSlice := mmapToIndex(capMem, 0, uint64(_nSize))
	store.Capacity = capSlice[0]
	capMem.Unmap()

	// Init the header
	headerSize := (store.Capacity + 2) * _nSize // Size of array + offset table in bytes
	store.headerMemory, err = mmap.MapRegion(store.file, int(headerSize), mmap.RDWR, 0, 0)
	utils.Check(err)
	index := mmapToIndex(store.headerMemory, 0, headerSize)
	store.index = index[1:]

	// Find the size of the array
	for i, offset := range index {
		// Look for the end of our written index
		if offset == 0 {
			store.Size = uint64(i - 2) // We're one past the end, and the end is one past size
			break
		}
	}
	// If we didn't find an end, we're full and we'll switch to read-only mode
	if store.Size == 0 {
		store.Size = store.Capacity
		// store.switchToReadOnly()
	} else {
		_, err = store.file.Seek(int64(store.index[store.Size]), os.SEEK_SET)
		utils.Check(err)
	}
	return &store
}

// STORAGE
func (store *FileStorage) init() *FileStorage {
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
	if uint64(index) != store.Size {
		return fmt.Errorf("Out of order message. Expected %d but got %d", store.Size, index)
	} else if index < 0 || uint64(index) >= store.Capacity {
		return fmt.Errorf("Index %d out of bounds [0, %d]", index, store.Capacity)
	}
	_, err := store.file.Write(data)
	if err != nil {
		return err
	}
	store.index[index+1] = store.index[index] + uint64(len(data))
	store.Size++
	return nil
}

// Return a reader pointing to the beginning of the message with the given index
func (store *FileStorage) ReaderAt(messageIndex uint64) (io.ReadCloser, error) {
	if uint64(messageIndex) >= store.Size {
		return nil, fmt.Errorf("Index %d exceeds available size of %d", messageIndex, store.Size)
	} else if messageIndex < 0 || uint64(messageIndex) >= store.Capacity {
		return nil, fmt.Errorf("Index %d out of bounds [0, %d]", messageIndex, store.Capacity)
	}

	r, err := os.Open(fname(store.fileId, store.rootPath))
	if err != nil {
		return nil, err
	}
	_, err = r.Seek(int64(store.index[messageIndex]), os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Return the size in bytes of the message at the given index
func (store *FileStorage) SizeOf(messageIndex uint64) (uint64, error) {
	if uint64(messageIndex) >= store.Size {
		return 0, fmt.Errorf("Index %d exceeds available size of %d", messageIndex, store.Size)
	} else if messageIndex < 0 || uint64(messageIndex) >= store.Capacity {
		return 0, fmt.Errorf("Index %d out of bounds [0, %d]", messageIndex, store.Capacity)
	}
	top := store.index[messageIndex+1]
	bottom := store.index[messageIndex]
	// if bottom > top {
	// 	return 0, fmt.Errorf("[%s.sizeOf(%d)] Top offset %d less than bottom %d", store.fileId, messageIndex, top, bottom)
	// }
	return top - bottom, nil
}

func (store *FileStorage) IsFull() bool {
	return store.Size == store.Capacity
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

func (store *FileStorage) switchToReadOnly() {
	index := make([]uint64, 0, store.Capacity)
	copy(index, store.index)
	store.index = index
	store.headerMemory.Unmap()
	store.file.Close()
}

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

func exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
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
