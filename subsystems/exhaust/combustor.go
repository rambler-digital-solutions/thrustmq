package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"runtime"
)

// 1. Pushes record into connection channel
// 2. Notifies turbine to flush changed record to disk
func forward(record *common.Record, connection *common.ConnectionStruct) {
	record.Connection = connection.ID
	record.Enqueued = common.TimestampUint64()
	record.Retries++
	record.Dirty = true

	oprecord := common.OplogRecord{Subsystem: "combustor"}

	oprecord.Message = fmt.Sprintf("fwd %d to connection %d (%d retries)", record.Seek, connection.ID, record.Retries)
	common.OplogChannel <- oprecord
	connection.Channel <- record

	oprecord.Message = fmt.Sprintf("fwd %d to turbine", record.Seek)
	common.OplogChannel <- oprecord
	TurbineChannel <- record
}

// Forwards records to connections or discards them
func combustor() {
	for {
		select {
		case record := <-CombustorChannel:
			if record.Enqueued == 0 {
				// Round robin connections with matching BucketID
				connection := nextConnFor(record.Bucket)
				if connection != nil && len(connection.Channel) != cap(connection.Channel) {
					forward(record, connection)
				}
			} else {
				oprecord := common.OplogRecord{Subsystem: "combustor"}
				oprecord.Message = fmt.Sprintf("record %v was already enqueued at %d... skipping...", record, record.Enqueued)
				oprecord.Send()
			}
		default:
			runtime.Gosched()
		}
	}
}
