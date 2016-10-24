package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"os"
	"runtime"
)

func turbine() {
	file, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer file.Close()

	for {
		RecordsMutex.Lock()
		for _, record := range RecordsMap {
			ProcessRecord(record, file)
		}
		RecordsMutex.Unlock()

		runtime.Gosched()
	}
}

func ProcessRecord(record *common.Record, file *os.File) {
	if record.Dirty {
		flushToDisk(file, record)
	}
	if record.Delivered != 0 {
		log.Print("delete", record)
		delete(RecordsMap, record.Seek)
	} else {
		if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
			enqueueAgain(record)
		}
	}
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
