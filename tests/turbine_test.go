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

func TestTurbineFlush(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := &common.Record{}
	record.Created = uint64(rand.Int63())
	record.Dirty = true
	exhaust.MapRecord(record)

	time.Sleep(1e7)

	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	recordOnDisk := &common.Record{}
	recordOnDisk.Deserialize(indexFile)

	recordInMemory := exhaust.RecordsMapGet(0)
	if recordOnDisk.Created != recordInMemory.Created {
		t.Fatalf("record on disk has wrong Created field %d (%d expected)", recordOnDisk.Created, exhaust.RecordsMap[0].Created)
	}
	if recordInMemory.Dirty {
		t.Fatalf("record wasn't marked as 'clear' in RecordMap =(")
	}
}

func TestTurbineRemoveSent(t *testing.T) {
	helper.BootstrapExhaust(t)
	exhaust.ClearRecordsMap()
	record := &common.Record{}
	record.Delivered = common.TimestampUint64()
	exhaust.MapRecord(record)
	time.Sleep(1e5)
	if exhaust.RecordsMapGet(record.Seek) != nil {
		t.Fatalf("record wasn't deleted %v", record)
	}
}

func TestTurbineRequeueOnDeadConnection(t *testing.T) {
	helper.BootstrapExhaust(t)
	helper.CheckCombustor(t, 0)
	helper.CheckRecordsMap(t, 0)

	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)

	bucket := uint64(rand.Int63())
	connectionId := uint64(rand.Int63())
	deadConnectionId := uint64(rand.Int63())

	record := &common.Record{}
	record.Bucket = bucket
	record.Connection = deadConnectionId
	record.Enqueued = common.TimestampUint64()

	helper.ForgeConnection(t, connectionId, bucket)
	exhaust.MapRecord(record)

	time.Sleep(1e7)

	exhaust.ProcessRecord(record, indexFile)

	retries := exhaust.RecordsMapGet(0).Retries
	if retries != 1 {
		t.Fatalf("record wasn't Enqueued (%d retries) / combustor %d / recordsMap %d", retries, len(exhaust.CombustorChannel), len(exhaust.RecordsMap))
	}

	enqueuedToConnection := len(exhaust.ConnectionsMapGet(connectionId).Channel)
	if enqueuedToConnection != 1 {
		t.Fatalf("record wasn't added to connection queue (%d items)", enqueuedToConnection)
	}
}
