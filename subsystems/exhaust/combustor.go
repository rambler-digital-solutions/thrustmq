package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"os"
	"runtime"
	"strconv"
)

var combustorThreshold = config.Exhaust.CombustionBuffer / 2
var turbineThreshold = config.Exhaust.TurbineBuffer / 2

func combustion() {
	for {
		if len(TurbineChannel) < turbineThreshold && len(CombustorChannel) < combustorThreshold && State.Tail < State.Head {
			burst()
		} else {
			runtime.Gosched()
		}
	}
}

func burst() {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()

	dataFile, err := os.OpenFile(config.Base.Data, os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer dataFile.Close()

	stat, err := indexFile.Stat()
	State.Head = uint64(stat.Size())

	logging.Debug("bursting", strconv.Itoa(int(State.Tail)), strconv.Itoa(int(State.Head)))
	for ptr := State.Tail; ptr <= State.Head-common.IndexSize; ptr += common.IndexSize {
		_, err = indexFile.Seek(int64(ptr), os.SEEK_SET)
		common.FaceIt(err)

		record := common.IndexRecord{}
		record.Deserialize(indexFile)

		burn(record, dataFile)
	}
}

func burn(record common.IndexRecord, dataFile *os.File) {
	if record.Sent != 0 {
		if _, ok := ConnectionsMap[record.Connection]; ok {
			return
		}
	}
	CombustorChannel <- &record
	record.Enqueued = common.TimestampUint64()
	record.Retries++
	TurbineChannel <- &record
}
