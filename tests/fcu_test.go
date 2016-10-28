package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Test that FCU instantiates records from disk
func TestInstantiationOfStoredMessages(t *testing.T) {
	numberOfRecords := 3
	connectionID := uint64(rand.Int63())
	bucketID := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketID
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	helper.ForgeConnection(t, connectionID, bucketID)
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionID, numberOfRecords)

	exhaust.DeleteConnectionByID(connectionID)
	if common.State.NextWriteOffset < common.State.UndeliveredOffset {
		t.Fatalf("head %d lt tail %d", common.State.NextWriteOffset, common.State.UndeliveredOffset)
	}

}

// Test that FCU instantiates ONLY valID messages
func TestInstantiationOfUndeliveredMessages(t *testing.T) {
	numberOfRecords := 4
	connectionID := uint64(rand.Int63())
	bucketID := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketID
		if i%2 == 0 {
			records[i].Delivered = common.TimestampUint64()
		}
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	helper.ForgeConnection(t, connectionID, bucketID)
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionID, numberOfRecords/2)
	exhaust.DeleteConnectionByID(connectionID)
	if common.State.NextWriteOffset < common.State.UndeliveredOffset {
		t.Fatalf("head %d lt tail %d", common.State.NextWriteOffset, common.State.UndeliveredOffset)
	}
}

// Test that FCU moves UndeliveredOffset
func TestMovementOfUndeliveredOffset(t *testing.T) {
	numberOfRecords := 4
	connectionID := uint64(rand.Int63())
	bucketID := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketID
		if i < numberOfRecords-1 {
			records[i].Delivered = common.TimestampUint64()
		}
	}
	helper.BootstrapExhaust(t)
	helper.DumpRecords(records)
	helper.ForgeConnection(t, connectionID, bucketID)
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e8)

	exhaust.DeleteConnectionByID(connectionID)
	if common.State.UndeliveredOffset != common.State.NextWriteOffset-common.IndexSize {
		t.Fatalf("min offset does not move %d - %d", common.State.UndeliveredOffset, common.State.NextWriteOffset)
	}
}

// Test that FCU removes old files
func TestFCUFileDeletion(t *testing.T) {
	numberOfRecords := int(config.Base.ChunkSize + 1)
	connectionID := uint64(rand.Int63())
	bucketID := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketID
		records[i].Delivered = common.TimestampUint64()
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	common.State.NextWriteOffset = common.IndexSize * uint64(numberOfRecords)
	common.State.UndeliveredOffset = common.State.NextWriteOffset

	time.Sleep(1e8)

	exhaust.DeleteConnectionByID(connectionID)
	_, err := os.Stat(config.Base.IndexPrefix + "0")
	if !os.IsNotExist(err) {
		t.Fatalf("index file still exists!")
	}
	_, err = os.Stat(config.Base.DataPrefix + "0")
	if !os.IsNotExist(err) {
		t.Fatalf("data file still exists!")
	}
}
