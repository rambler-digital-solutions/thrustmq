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

// check that turbine requeues messages from dead connections
// check that turbine removes sent messages

func TestTurbineFlush(t *testing.T) {
	helper.BootstrapExhaust(t)

	exhaust.RecordsMap[0] = &common.Record{}
	exhaust.RecordsMap[0].Created = uint64(rand.Int63())
	exhaust.RecordsMap[0].Dirty = true

	time.Sleep(1e7)

	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	recordOnDisk := &common.Record{}
	recordOnDisk.Deserialize(indexFile)

	if recordOnDisk.Created != exhaust.RecordsMap[0].Created {
		t.Fatalf("record on disk has wrong Created field %d (%d expected)", recordOnDisk.Created, exhaust.RecordsMap[0].Created)
	}
	if exhaust.RecordsMap[0].Dirty {
		t.Fatalf("record wasn't marked as 'clear' in RecordMap =(")
	}
}
