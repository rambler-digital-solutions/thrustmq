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
		if len(CombustorChannel) < cap(CombustorChannel)/2 && common.State.Tail < common.State.Head {
			stat, err := indexFile.Stat()
			common.FaceIt(err)
			common.State.Head = uint64(stat.Size())
			_, err = indexFile.Seek(int64(common.State.Tail), os.SEEK_SET)
			common.FaceIt(err)
			reader := bufio.NewReaderSize(indexFile, config.Base.NetworkBuffer)
			inject(reader)
		} else {
			runtime.Gosched()
		}
	}
}

func inject(reader *bufio.Reader) {
	for ptr := common.State.Tail; ptr <= common.State.Head-common.IndexSize; ptr += common.IndexSize {
		record := &common.Record{}
		record.Deserialize(reader)
		record.Seek = ptr
		if !RecordInMemory(record) {
			MapRecord(record)
			CombustorChannel <- record
		}
	}
}
