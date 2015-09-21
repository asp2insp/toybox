package track

import (
	"fmt"
	"os"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

func TestInitTrack(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	defer track.Close()

	testutils.CheckString("id", track.Id, t)
	testutils.CheckString("", track.RootPath, t)
	testutils.ExpectTrue(track.alive, "Expected track to be alive", t)
}

func TestGetReader(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	defer track.Close()

	_, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)

	// Test that we can ask for a future offset
	_, err = track.ReaderAt(100)
	testutils.CheckErr(err, t)
}

func TestReadWriteTrack(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	defer track.Close()
	err := track.WriteMessage(testData)
	testutils.CheckErr(err, t)

	r, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)

	temp := make([]byte, 100)
	n1, err := r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp[0:n1], t)
}

func _TestPersistenceOfTrack(t *testing.T) {
	cleanupTrack()
	track := NewFileStorage("", "id", 10)
	track.WriteMessage(0, testData)
	track.Close()

	track = Open("", "id")
	testutils.CheckUint64(10, track.Capacity, t)
	testutils.CheckUint64(1, track.Size, t)

	r, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)
	temp := make([]byte, len(testData))
	n1, err := r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp, t)
}

func TestFillUpMultipleFiles(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	defer track.Close()
	var err error
	var i uint64
	for i = 0; i < 3*CHUNK_SIZE; i++ {
		err = track.WriteMessage([]byte(fmt.Sprintf("%d", i)))
		testutils.CheckErr(err, t)
	}

	temp := make([]byte, 100)
	r, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)
	for i = 0; i < 3*CHUNK_SIZE; i++ {
		n1, err := r.Read(temp)
		testutils.CheckErr(err, t)
		testutils.CheckByteSlice([]byte(fmt.Sprintf("%d", i)), temp[0:n1], t)
	}

	// track = Open("", "id")
	// testutils.CheckUint64(10, track.Capacity, t)
	// testutils.CheckUint64(10, track.Size, t)

	// r, err := track.ReaderAt(0)
	// testutils.CheckErr(err, t)
	// temp := make([]byte, len(testData))

	// for i := 0; i < 10; i++ {
	// 	n1, err := r.Read(temp)

	// 	testutils.CheckInt(len(testData), n1, t)
	// 	testutils.CheckErr(err, t)
	// 	testutils.CheckByteSlice(testData, temp, t)
	// }
}

func cleanupTrack() {
	os.Remove(fname("id", ""))
	os.Remove(fname("id0", ""))
	os.Remove(fname("id1", ""))
	os.Remove(fname("id2", ""))
}
