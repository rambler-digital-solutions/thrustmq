package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"runtime"
)

func forward(record *common.Record) {
	if record.Enqueued > 0 {
		return
	}
	connection := nextConnFor(record.Bucket)
	if connection == nil {
		return
	}
	if len(connection.Channel) != cap(connection.Channel) {
		record.Connection = connection.ID
		record.Enqueued = common.TimestampUint64()
		record.Retries++
		record.Dirty = true
		TurbineChannel <- record
		connection.Channel <- record
	}
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
