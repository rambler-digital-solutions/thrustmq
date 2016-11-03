package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
)

// If there is no consumer for the bucket - just discard the record
func TestCombustorDiscardByBucket(t *testing.T) {
	common.Log("test", "\n\nTestCombustorDiscardByBucket")
	exhaust.ClearRecordsMap()
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Bucket = uint64(rand.Int63())
	exhaust.MapRecord(record)
	exhaust.CombustorChannel <- record

	helper.CheckRecordsMap(t, 1)

	helper.WaitForCombustor()
	helper.WaitForAfterburner()

	helper.CheckCombustor(t, 0)
	helper.CheckRecordsMap(t, 0)
}

// Records from one bucket must be assigned to consumers of this bucket evenly
func TestCombustorRoundRobinBuckets(t *testing.T) {
	common.Log("test", "\n\nTestCombustorRoundRobinBuckets")
	exhaust.ClearRecordsMap()
	helper.BootstrapExhaust(t)
	helper.CheckConnections(t, 0)

	bucket := uint64(rand.Int63())
	clientsCount := 2
	recordsCount := 4

	clients := make([]*common.ConnectionStruct, clientsCount)
	for i := 0; i < clientsCount; i++ {
		clients[i] = helper.ForgeConnection(uint64(i), bucket)
	}

	for i := 0; i < recordsCount; i++ {
		record := helper.ForgeAndMapRecord(uint64(i)*common.IndexSize, bucket)
		exhaust.CombustorChannel <- record
	}

	helper.WaitForCombustor()

	helper.CheckCombustor(t, 0)

	for i := 0; i < clientsCount; i++ {
		if len(clients[i].Channel) != recordsCount/2 {
			t.Fatalf("client #%d has %d records in channel (instead of %d)", i, len(clients[i].Channel), recordsCount/2)
		}
	}
}
