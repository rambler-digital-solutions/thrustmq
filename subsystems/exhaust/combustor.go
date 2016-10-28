package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"runtime"
)

// 1. Pushes record into connection channel
// 2. Notifies turbine to flush changed record to disk
func forward(record *common.Record, connection *common.ConnectionStruct) {
	record.Connection = connection.ID
	record.Enqueued = common.TimestampUint64()
	record.Retries++
	record.Dirty = true
	if config.Base.Debug {
		log.Print("to turbine and connection!~ ", record)
	}
	TurbineChannel <- record
	connection.Channel <- record
	if config.Base.Debug {
		log.Print("done~ ")
	}
}

// Forwards records to connections or discards them
func combustor() {
	for {
		select {
		case record := <-CombustorChannel:
			if config.Base.Debug {
				log.Print("forward ", record)
			}
			if record.Enqueued == 0 {
				// Round robin connections with matching BucketID
				connection := nextConnFor(record.Bucket)
				if connection != nil && len(connection.Channel) != cap(connection.Channel) {
					forward(record, connection)
				}
			}
		default:
			runtime.Gosched()
		}
	}
}
