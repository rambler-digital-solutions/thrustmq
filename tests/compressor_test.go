package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"os"
	"testing"
	"time"
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
func TestChunkSwitching(t *testing.T) {
	helper.BootstrapIntake(t)

	common.State.IndexOffset = 0

	for i := 0; i < int(config.Base.ChunkSize+1); i++ {
		message := &common.IntakeStruct{}
		message.Record = &common.Record{}
		intake.Stage2CompressorChannel <- message
	}

	time.Sleep(1e7)

	helper.CheckUncompressedMessages(t, 0)
	helper.CheckChunkNumber(t, 1)
}

// Check that chunks are being circularly overwritten
func TestChunkOverride(t *testing.T) {
	helper.BootstrapIntake(t)

	common.State.IndexOffset = config.Base.ChunkSize * (config.Base.MaxChunks - 1) * common.IndexSize

	for i := 0; i < int(config.Base.ChunkSize+1); i++ {
		message := &common.IntakeStruct{}
		message.Record = &common.Record{Bucket: 666}
		intake.Stage2CompressorChannel <- message
	}

	time.Sleep(1e7)

	helper.CheckUncompressedMessages(t, 0)
	helper.CheckChunkNumber(t, 0)
}
