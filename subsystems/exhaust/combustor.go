package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"os"
	"runtime"
	"strconv"
)

func forward() {
	// grab messages in channel & pass them to nozzles

	// if there's no connection for this bucket - do nothing
	// else send it to the nozzle (RR), add dirty sent record to turbine (with retries++)

	// same logic for file reader

	// record.Enqueued = common.TimestampUint64()
	// record.Retries++
}

func combustion() {
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
		if len(TurbineChannel) < turbineThreshold && len(CombustorChannel) < combustorThreshold && State.Tail < State.Head {
			burn(getReader(), dataFile)
		} else {
			runtime.Gosched()
		}
	}
}

func getReader() {
	stat, err := indexFile.Stat()
	common.FaceIt(err)
	State.Head = uint64(stat.Size())
	_, err = indexFile.Seek(State.Tail, os.SEEK_SET)
	common.FaceIt(err)
	indexReader = bufio.NewReaderSize(reader, config.Base.NetworkBuffer)
}

func burn() {
	logging.Debug("bursting", strconv.Itoa(int(State.Tail)), strconv.Itoa(int(State.Head)))

	for ptr := State.Tail; ptr <= State.Head-common.IndexSize; ptr += common.IndexSize {
		record := common.Record{}
		record.Deserialize(indexFile)
		record.Seek = ptr
		forward(record)
	}
}
