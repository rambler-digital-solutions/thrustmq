package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
	"time"
)

// If there is no consumer for the bucket - just discard the record
func TestCombustorDiscardByBucket(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Created = uint64(rand.Int63())
	record.Bucket = uint64(rand.Int63())
	exhaust.MapRecord(record)

	exhaust.CombustorChannel <- record

	time.Sleep(1e6)

	helper.CheckRecordsMap(t, 0)
}
