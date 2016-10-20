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
		recordsCount := len(TurbineChannel)
		if recordsCount > 0 {
			mark(recordsCount, file)
		} else {
			runtime.Gosched()
		}
	}
}

func mark(notificationsLen int, file *os.File) {
	hash := make(map[uint64]*common.Record)

	for i := 0; i < notificationsLen; i++ {
		recordFromChannel := <-TurbineChannel
		merge(hash, recordFromChannel, file)
	}

	flush(hash, file)
}

func merge(hash map[uint64]*common.Record, record *common.Record, file *os.File) {
	if _, ok := hash[record.Seek]; ok {
		hash[record.Seek].Merge(record)
	} else {
		_, err := file.Seek(int64(record.Seek), os.SEEK_SET)
		common.FaceIt(err)
		hash[record.Seek] = &common.Record{}
		hash[record.Seek].Deserialize(file)
	}
}

func flush(hash map[uint64]*common.Record, file *os.File) {
	for _, record := range hash {
		_, err := file.Seek(int64(record.Seek), os.SEEK_SET)
		common.FaceIt(err)
		file.Write(record.Serialize())
	}
}
