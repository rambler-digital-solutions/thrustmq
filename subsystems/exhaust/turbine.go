package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

func routeRecord(record *common.Record) {
	if record.Delivered != 0 {
		delete(RecordsMap, record.Seek)
	} else {
		if !connectionAlive(record.Connection) {
			CombustorChannel <- record
		}
	}
}

func turbine() {
	file, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer file.Close()

	for {
		for _, record := range RecordsMap {
			if record.Dirty {
				flush(file, record)
				record.Dirty = false
			}

			routeRecord(record)
		}

		runtime.Gosched()
	}
}

func flush(file *os.File, record *common.Record) {
	_, err := file.Seek(int64(record.Seek), os.SEEK_SET)
	common.FaceIt(err)
	file.Write(record.Serialize())
}
