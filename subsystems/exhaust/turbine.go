package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

func turbine() {
	for {
		RecordsMutex.Lock()
		for _, record := range RecordsMap {
			ProcessRecord(record)
		}
		RecordsMutex.Unlock()

		runtime.Gosched()
	}
}

func ProcessRecord(record *common.Record) {
	if record.Delivered != 0 || !BucketRequired(record.Bucket) {
		delete(RecordsMap, record.Seek)
	} else {
		if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
			record.Enqueued = 0
			record.Connection = 0
			CombustorChannel <- record
		}
	}
}

func turbineStage2() {
	file, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer file.Close()

	for {
		record := <-TurbineChannel
		if record.Dirty {
			_, err := file.Seek(int64(record.Seek), os.SEEK_SET)
			common.FaceIt(err)
			file.Write(record.Serialize())
			record.Dirty = false
			file.Sync()
		}
	}
}
