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

// 1. Create dirty record
// 2. Wait for turbine to flush it
// 3. Check that record was actually updated on disk
func TestTurbineFlush(t *testing.T) {
	helper.BootstrapExhaust(t)

	record := helper.ForgeAndMapRecord(uint64(0), uint64(rand.Int63()))
	record.Created = uint64(rand.Int63())
	record.Dirty = true
	exhaust.TurbineChannel <- record

	time.Sleep(config.Base.TestDelayDuration)

	indexFile, err := os.OpenFile(config.Base.IndexPrefix+common.State.StringChunkNumber(), os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()

	recordOnDisk := &common.Record{}
	recordOnDisk.Deserialize(indexFile)

	if recordOnDisk.Created != record.Created {
		t.Fatalf("record on disk has wrong Created field %d (%d expected)", recordOnDisk.Created, record.Created)
	}
	if record.Dirty {
		t.Fatalf("record wasn't marked as 'clear' in RecordMap =(")
	}
}
