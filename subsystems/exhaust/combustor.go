package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
)

func forward(record *common.Record) {
	if record.Enqueued > 0 {
		return
	}
	ConnectionsMutex.RLock()
	for _, connection := range ConnectionsMap {
		if connection.Bucket == record.Bucket && len(connection.Channel) != cap(connection.Channel) {
			record.Connection = connection.Id
			record.Enqueued = common.TimestampUint64()
			record.Retries++
			record.Dirty = true
			TurbineChannel <- record
			connection.Channel <- record
		}
	}
	ConnectionsMutex.RUnlock()
}

func combustor() {
	for {
		select {
		case record := <-CombustorChannel:
			forward(record)
		default:
			runtime.Gosched()
		}
	}
}

func afterburner() {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()

	for {
		if len(CombustorChannel) < cap(CombustorChannel)/2 && common.State.Tail < common.State.Head {
			reader := getReader(indexFile)
			burn(reader)
		} else {
			runtime.Gosched()
		}
	}
}

func getReader(indexFile *os.File) *bufio.Reader {
	stat, err := indexFile.Stat()
	common.FaceIt(err)
	common.State.Head = uint64(stat.Size())
	_, err = indexFile.Seek(int64(common.State.Tail), os.SEEK_SET)
	common.FaceIt(err)
	return bufio.NewReaderSize(indexFile, config.Base.NetworkBuffer)
}

func burn(reader *bufio.Reader) {
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
