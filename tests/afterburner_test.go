package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
	"time"
)

// When record was delivered - remove it from memory
func TestAfterburnerRemoveSent(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Seek = common.TimestampUint64()
	record.Delivered = common.TimestampUint64()
	exhaust.MapRecord(record)

	time.Sleep(1e6)

	if exhaust.RecordsMapGet(record.Seek) != nil {
		t.Fatalf("record wasn't deleted %v", exhaust.RecordsMapGet(record.Seek))
	}
}

// When connection with record dies - requeue the record
func TestAfterburnerRequeueOnDeadConnection(t *testing.T) {
	helper.BootstrapExhaust(t)

	seek := uint64(rand.Int63())
	bucketId := uint64(rand.Int63())
	connectionId := uint64(rand.Int63())
	deadConnectionId := uint64(rand.Int63())
	helper.ForgeConnection(t, connectionId, bucketId)

	record := &common.Record{}
	record.Seek = seek
	record.Bucket = bucketId
	record.Connection = deadConnectionId
	record.Enqueued = common.TimestampUint64()
	exhaust.MapRecord(record)
	exhaust.AfterburnerChannel <- record

	time.Sleep(1e6)

	retries := exhaust.RecordsMapGet(seek).Retries
	if retries != 1 {
		t.Fatalf("record wasn't Enqueued (%d retries)", retries)
	}

	enqueuedToConnection := len(exhaust.ConnectionsMapGet(connectionId).Channel)
	if enqueuedToConnection != 1 {
		t.Fatalf("record wasn't added to connection queue (%d items)", enqueuedToConnection)
	}

	exhaust.DeleteConnectionById(connectionId)
}
