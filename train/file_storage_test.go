package track

import (
	"os"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

var testData = []byte("0123456789ABCDEF")

func TestInit(t *testing.T) {
	cleanup()
	store := NewFileStorage("", "id", 10)
	defer store.Close()

	testutils.CheckString("id", store.fileId, t)
	testutils.CheckString("", store.rootPath, t)

	if store.file == nil {
		t.Errorf("No main file opened for %s", fname("id", ""))
	}

	testutils.CheckUint64(10, store.Capacity, t)
	testutils.CheckUint32(0, store.Size, t)

	// Test internals
	testutils.CheckInt(11, len(store.index), t)
}

func TestReadWrite(t *testing.T) {
	cleanup()
	store := NewFileStorage("", "id", 10)
	defer store.Close()
	err := store.WriteMessage(0, testData)
	testutils.CheckErr(err, t)

	// Size = 8 bytes
	// Index = 8 bytes * 11
	// Offset of first item should be 96
	testutils.CheckUint64(96, store.index[0], t)
	testutils.CheckUint64(96+uint64(len(testData)), store.index[1], t)

	err = store.WriteMessage(1, testData[:4])
	testutils.CheckErr(err, t)

	store.Flush()

	r, err := store.ReaderAt(0)
	testutils.CheckErr(err, t)
	s, err := store.SizeOf(0)
	testutils.CheckErr(err, t)

	testutils.CheckUint64(uint64(len(testData)), s, t)
	temp := make([]byte, len(testData))
	n1, err := r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp, t)
}

func _TestPersistence(t *testing.T) {
	cleanup()
	store := NewFileStorage("", "id", 10)
	store.WriteMessage(0, testData)
	store.Close()

	store = NewFileStorage("", "id", 10)
}

func cleanup() {
	os.Remove(fname("id", ""))
}
