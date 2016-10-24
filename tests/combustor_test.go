package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
	"time"
)

func TestCombustorDiscardByBucket(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Created = uint64(rand.Int63())
	record.Bucket = uint64(rand.Int63())

	exhaust.RecordsMutex.Lock()
	exhaust.RecordsMap[0] = record
	exhaust.RecordsMutex.Unlock()

	exhaust.CombustorChannel <- record

	time.Sleep(1e6)

	helper.CheckRecordsMap(t, 0)
}

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
	common.State.Tail = 0
	common.State.Head = common.IndexSize * uint64(numberOfRecords-1)

	time.Sleep(1e7)

	helper.CheckConnectionChannel(t, connectionId, numberOfRecords)

	if common.State.Head < common.State.Tail {
		t.Fatalf("head %d lt tail %d", common.State.Head, common.State.Tail)
	}

	exhaust.DeleteConnectionById(connectionId)
}
