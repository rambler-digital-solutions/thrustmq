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
	helper.ForgeConnection(connectionID, bucketID)

	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(config.Base.TestDelayDuration)

	helper.CheckConnectionChannel(t, connectionID, numberOfRecords)

	exhaust.DeleteConnectionByID(connectionID)
	if common.State.WriteOffset < common.State.UndeliveredOffset {
		t.Fatalf("head %d lt tail %d", common.State.WriteOffset, common.State.UndeliveredOffset)
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
	helper.ForgeConnection(connectionID, bucketID)
	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(config.Base.TestDelayDuration)

	helper.CheckConnectionChannel(t, connectionID, numberOfRecords/2)
	exhaust.DeleteConnectionByID(connectionID)
	if common.State.WriteOffset < common.State.UndeliveredOffset {
		t.Fatalf("head %d lt tail %d", common.State.WriteOffset, common.State.UndeliveredOffset)
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
	helper.ForgeConnection(connectionID, bucketID)
	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(config.Base.TestDelayDuration)

	exhaust.DeleteConnectionByID(connectionID)
	if common.State.UndeliveredOffset != common.State.WriteOffset-common.IndexSize {
		t.Fatalf("min offset does not move %d - %d", common.State.UndeliveredOffset, common.State.WriteOffset)
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
	common.State.WriteOffset = common.IndexSize * uint64(numberOfRecords)
	common.State.UndeliveredOffset = common.State.WriteOffset

	time.Sleep(config.Base.TestDelayDuration)

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
