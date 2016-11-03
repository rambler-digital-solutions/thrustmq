package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
	"time"
)

// When record was delivered - remove it from memory
func fTestAfterburnerRemoveSent(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Seek = common.TimestampUint64()
	record.Delivered = common.TimestampUint64()
	exhaust.MapRecord(record)

	time.Sleep(config.Base.TestDelayDuration)

	if exhaust.RecordsMapGet(record.Seek) != nil {
		t.Fatalf("record wasn't deleted %v", exhaust.RecordsMapGet(record.Seek))
	}
}

// When connection with record dies - requeue the record
func TestAfterburnerRequeueOnDeadConnection(t *testing.T) {
	helper.BootstrapExhaust(t)

	seek := uint64(rand.Int63())
	bucketID := uint64(rand.Int63())
	connectionID := uint64(rand.Int63())
	deadConnectionID := uint64(rand.Int63())
	client := helper.ForgeConnection(connectionID, bucketID)

	record := helper.ForgeAndMapRecord(seek, bucketID)
	record.Connection = deadConnectionID
	record.Enqueued = common.TimestampUint64()
	exhaust.AfterburnerChannel <- record

	time.Sleep(config.Base.TestDelayDuration)
	helper.WaitForAfterburner()
	helper.WaitForCombustor()
	helper.WaitForTurbine()

	retries := exhaust.RecordsMapGet(seek).Retries
	if retries != 1 {
		t.Fatalf("record wasn't Enqueued (%d retries)", retries)
	}

	if len(client.Channel) != 1 {
		t.Fatalf("record wasn't added to connection queue (%d items)", len(client.Channel))
	}

	exhaust.DeleteConnectionByID(connectionID)
}
