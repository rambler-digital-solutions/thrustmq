package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"testing"
)

// Write single record on disk, then read it and check if they are the same
func TestRecordSerialization(t *testing.T) {
	indexFile, err := os.OpenFile(config.Base.Index+"_test", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()

	record := common.Record{}

	slots := record.Slots()
	for i := 0; i < len(slots); i++ {
		*slots[i] = uint64(i + 1)
	}

	indexFile.Seek(0, os.SEEK_SET)
	indexFile.Write(record.Serialize())

	indexFile.Seek(0, os.SEEK_SET)
	readRecord := common.Record{}
	readRecord.Deserialize(indexFile)

	readSlots := readRecord.Slots()
	for i := 0; i < len(slots); i++ {
		if *slots[i] != *readSlots[i] {
			t.Fatalf("deserialized field %d ne expected %d", *readSlots[i], *slots[i])
		}
	}
}

// Writes several chunks of records and checks that
// 1. files were created
// 2. position pointer is set correctly
func TestRecordsChunking(t *testing.T) {
}
