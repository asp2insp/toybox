package track

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/asp2insp/go-misc/testutils"
	"github.com/asp2insp/go-misc/utils"
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

func TestPersistenceOfTrack(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	err := track.WriteMessage(testData)
	testutils.CheckErr(err, t)
	err = track.WriteMessage(testData)
	testutils.CheckErr(err, t)

	// wait for writes to occur
	for len(track.stores) == 0 || track.stores[0].Size < 2 {
		time.Sleep(100 * time.Millisecond)
	}
	testutils.CheckInt(1, len(track.stores), t)
	testutils.CheckUint64(2, track.stores[0].Size, t)

	track.Close()

	track = OpenTrack("", "id")
	defer track.Close()
	testutils.CheckInt(1, len(track.stores), t)
	testutils.CheckUint64(2, track.stores[0].Size, t)

	r, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)
	temp := make([]byte, len(testData))
	n1, err := r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp, t)

	n1, err = r.Read(temp)

	testutils.CheckInt(len(testData), n1, t)
	testutils.CheckErr(err, t)
	testutils.CheckByteSlice(testData, temp, t)
}

func TestFillUpMultipleFiles(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")
	var err error
	var i uint64
	for i = 0; i < 3*CHUNK_SIZE; i++ {
		err = track.WriteMessage([]byte(fmt.Sprintf("%d", i)))
		testutils.CheckErr(err, t)
	}

	track.Close()
	track.WaitForShutdown()

	track = OpenTrack("", "id")
	defer track.Close()

	testutils.CheckInt(3, len(track.stores), t)
	for i := 0; i < 3; i++ {
		testutils.CheckUint64(CHUNK_SIZE, track.stores[i].Size, t)
		testutils.CheckUint64(CHUNK_SIZE, track.stores[i].Capacity, t)
	}

	temp := make([]byte, 100)
	r, err := track.ReaderAt(0)
	testutils.CheckErr(err, t)
	for i = 0; i < 3*CHUNK_SIZE; i++ {
		n1, err := r.Read(temp)
		testutils.CheckErr(err, t)
		testutils.CheckByteSlice([]byte(fmt.Sprintf("%d", i)), temp[0:n1], t)
	}
}

func TestConcurrentReadWrites(t *testing.T) {
	cleanupTrack()
	track := NewTrack("", "id")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		var i uint64
		for i = 0; i < CHUNK_SIZE; i++ {
			track.WriteMessage([]byte(fmt.Sprintf("%d", i)))
		}
		wg.Done()
	}()

	go func() {
		temp := make([]byte, 100)
		r, _ := track.ReaderAt(0)
		var i uint64
		for i = 0; i < CHUNK_SIZE; i++ {
			n1, err := r.Read(temp)
			utils.Check(err)
			testutils.CheckByteSlice([]byte(fmt.Sprintf("%d", i)), temp[0:n1], t)
		}
		wg.Done()
	}()

	wg.Wait()
}

func BenchmarkThroughput(b *testing.B) {
	cleanupTrack()
	b.ResetTimer()
	track := NewTrack("", "id")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		var i uint64
		n := uint64(b.N)
		for i = 0; i < n; i++ {
			track.WriteMessage([]byte(fmt.Sprintf("%d", i)))
			// track.WriteMessage([]byte("Hello World"))
		}
		wg.Done()
	}()

	go func() {
		temp := make([]byte, 100)
		r, _ := track.ReaderAt(0)
		n := uint64(b.N)
		var i uint64
		for i = 0; i < n; i++ {
			_, err := r.Read(temp)
			if err != nil {
				fmt.Printf("\nErr %v, on message %d/%d\n", err, i, n)
			}
			utils.Check(err)
		}
		wg.Done()
	}()

	wg.Wait()
}

func cleanupTrack() {
	for i := 0; ; i++ {
		storeId := fmt.Sprintf("id%d", i)
		if !exists(fname(storeId, "")) {
			break
		}
		os.Remove(fname(storeId, ""))
	}
}
