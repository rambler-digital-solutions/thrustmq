package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

// Entry function, loops through the records in memory and take appropriate action
func turbineStage1() {
	for {
		RecordsMutex.Lock()
		for _, record := range RecordsMap {
			select {
			case AfterburnerChannel <- record:
			default:
			}
		}
		RecordsMutex.Unlock()
		runtime.Gosched()
	}
}

// Flushes "dirty" records to disk
func turbineStage2() {
	for {
		record := <-TurbineChannel
		if record.Dirty {
			path := config.Base.IndexPrefix + common.State.StringChunkNumber()
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
			common.FaceIt(err)
			_, err = file.Seek(int64(record.Seek), os.SEEK_SET)
			common.FaceIt(err)
			file.Write(record.Serialize())
			record.Dirty = false
			file.Close()
			common.Log("turbine", fmt.Sprintf("flushed record %d to disk (%d retries)", record.Seek, record.Retries))
		}
	}
}
