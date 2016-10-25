package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
	"time"
)

// Test that FCU instantiates records from disk
func TestInstantiationOfStoredMessages(t *testing.T) {
	numberOfRecords := 10
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
	common.State.MaxOffset = common.IndexSize * uint64(numberOfRecords-1)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionId, numberOfRecords)

	if common.State.MaxOffset < common.State.MinOffset {
		t.Fatalf("head %d lt tail %d", common.State.MaxOffset, common.State.MinOffset)
	}

	exhaust.DeleteConnectionById(connectionId)
}
