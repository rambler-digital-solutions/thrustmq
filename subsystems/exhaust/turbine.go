package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

func turbine() {
	file, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer file.Close()

	for {
		for _, record := range RecordsMap {
			ProcessRecord(record, file)
		}

		runtime.Gosched()
	}
}

func ProcessRecord(record *common.Record, file *os.File) {
	if record.Dirty {
		flushToDisk(file, record)
	}
	if record.Delivered != 0 {
		deleteRecord(record)
	} else {
		if record.Enqueued > 0 && !connectionAlive(record.Connection) {
			enqueueAgain(record)
		}
	}
}

func deleteRecord(record *common.Record) {
	RecordsMutex.Lock()
	delete(RecordsMap, record.Seek)
	RecordsMutex.Unlock()
}

func enqueueAgain(record *common.Record) {
	record.Enqueued = 0
	record.Connection = 0
	CombustorChannel <- record
}

func flushToDisk(file *os.File, record *common.Record) {
	_, err := file.Seek(int64(record.Seek), os.SEEK_SET)
	common.FaceIt(err)
	file.Write(record.Serialize())
	record.Dirty = false
}
