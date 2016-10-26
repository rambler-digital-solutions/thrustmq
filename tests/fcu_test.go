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
	connectionId := uint64(rand.Int63())
	bucketId := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketId
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	helper.ForgeConnection(t, connectionId, bucketId)
	common.State.MinOffset = 0
	common.State.IndexOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionId, numberOfRecords)

	exhaust.DeleteConnectionById(connectionId)
	if common.State.IndexOffset < common.State.MinOffset {
		t.Fatalf("head %d lt tail %d", common.State.IndexOffset, common.State.MinOffset)
	}

}

// Test that FCU instantiates ONLY valid messages
func TestInstantiationOfUndeliveredMessages(t *testing.T) {
	numberOfRecords := 4
	connectionId := uint64(rand.Int63())
	bucketId := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketId
		if i%2 == 0 {
			records[i].Delivered = common.TimestampUint64()
		}
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	helper.ForgeConnection(t, connectionId, bucketId)
	common.State.MinOffset = 0
	common.State.IndexOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionId, numberOfRecords/2)
	exhaust.DeleteConnectionById(connectionId)
	if common.State.IndexOffset < common.State.MinOffset {
		t.Fatalf("head %d lt tail %d", common.State.IndexOffset, common.State.MinOffset)
	}
}

// Test that FCU moves MinOffset
func TestMovementOfMinOffset(t *testing.T) {
	numberOfRecords := 4
	connectionId := uint64(rand.Int63())
	bucketId := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketId
		if i < numberOfRecords-1 {
			records[i].Delivered = common.TimestampUint64()
		}
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	helper.ForgeConnection(t, connectionId, bucketId)
	common.State.MinOffset = 0
	common.State.IndexOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	exhaust.DeleteConnectionById(connectionId)
	if common.State.MinOffset != common.State.IndexOffset-common.IndexSize {
		t.Fatalf("min offset does not move %d - %d", common.State.MinOffset, common.State.IndexOffset)
	}
}

// Test that FCU removes old files
func TestFCUFileDeletion(t *testing.T) {
	numberOfRecords := int(config.Base.ChunkSize + 1)
	connectionId := uint64(rand.Int63())
	bucketId := uint64(rand.Int63())
	records := make([]*common.Record, numberOfRecords)
	for i := 0; i < numberOfRecords; i++ {
		records[i] = &common.Record{}
		records[i].Bucket = bucketId
		records[i].Delivered = common.TimestampUint64()
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	common.State.MinOffset = 0
	common.State.IndexOffset = common.IndexSize * uint64(numberOfRecords)

	time.Sleep(1e7)

	exhaust.DeleteConnectionById(connectionId)
	_, err := os.Stat(config.Base.Index + "0")
	if !os.IsNotExist(err) {
		t.Fatalf("index file still exists!")
	}
	_, err = os.Stat(config.Base.Data + "0")
	if !os.IsNotExist(err) {
		t.Fatalf("data file still exists!")
	}
}
