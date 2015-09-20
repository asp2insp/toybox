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
	testutils.CheckUint64(0, store.Size, t)

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

func TestPersistence(t *testing.T) {
	cleanup()
	store := NewFileStorage("", "id", 10)
	store.WriteMessage(0, testData)
	store.Close()

	store = Open("", "id")
	testutils.CheckUint64(10, store.Capacity, t)
	testutils.CheckUint64(1, store.Size, t)

	r, err := store.ReaderAt(0)
	testutils.CheckErr(err, t)
	temp := make([]byte, len(testData))
	n1, err := r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp, t)
}

func TestFillUp(t *testing.T) {
	cleanup()
	store := NewFileStorage("", "id", 10)
	var err error
	for i := 0; i < 10; i++ {
		err = store.WriteMessage(i, testData)
		testutils.CheckErr(err, t)
	}
	store.Close()

	store = Open("", "id")
	testutils.CheckUint64(10, store.Capacity, t)
	testutils.CheckUint64(10, store.Size, t)

	r, err := store.ReaderAt(0)
	testutils.CheckErr(err, t)
	temp := make([]byte, len(testData))

	for i := 0; i < 10; i++ {
		n1, err := r.Read(temp)

		testutils.CheckInt(len(testData), n1, t)
		testutils.CheckErr(err, t)
		testutils.CheckByteSlice(testData, temp, t)
	}
	store.Close()
}

func cleanup() {
	os.Remove(fname("id", ""))
}
