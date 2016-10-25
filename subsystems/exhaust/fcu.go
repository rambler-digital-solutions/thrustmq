package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

func fuelControlUnit() {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()

	for {
		if len(CombustorChannel) < cap(CombustorChannel)/2 && common.State.MinOffset < common.State.MaxOffset {
			stat, err := indexFile.Stat()
			common.FaceIt(err)
			common.State.MaxOffset = uint64(stat.Size())
			_, err = indexFile.Seek(int64(common.State.MinOffset), os.SEEK_SET)
			common.FaceIt(err)
			reader := bufio.NewReaderSize(indexFile, config.Base.NetworkBuffer)
			inject(reader)
		} else {
			runtime.Gosched()
		}
	}
}

func inject(reader *bufio.Reader) {
	for ptr := common.State.MinOffset; ptr <= common.State.MaxOffset-common.IndexSize; ptr += common.IndexSize {
		record := &common.Record{}
		record.Deserialize(reader)
		record.Seek = ptr
		if !RecordInMemory(record) {
			MapRecord(record)
			CombustorChannel <- record
		}
	}
}
