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
	record.Dirty = true
	record.Retries++

	oprecord := common.OplogRecord{Subsystem: "combustor"}

	oprecord.Message = fmt.Sprintf("forward record %d to connection %d (%d retries)", record.Seek, connection.ID, record.Retries)
	common.OplogChannel <- oprecord
	connection.Channel <- record

	oprecord.Message = fmt.Sprintf("forward record %d to turbine", record.Seek)
	common.OplogChannel <- oprecord
	TurbineChannel <- record
}

// Forwards records to connections or discards them
func combustor() {
	oprecord := common.OplogRecord{Subsystem: "combustor"}
	for {
		select {
		case record := <-CombustorChannel:
			if record.Enqueued == 0 {
				// Round robin connections with matching BucketID
				connection := nextConnFor(record.Bucket)

				if connection == nil {
					oprecord.Message = fmt.Sprintf("skipping fwd of record %d (connection is nil)", record.Seek)
					oprecord.Send()
					continue
				}

				if len(connection.Channel) == cap(connection.Channel) {
					oprecord.Message = fmt.Sprintf("skipping fwd of record %d (connection %d is full)", record.Seek, connection.ID)
					oprecord.Send()
					continue
				}

				oprecord.Message = fmt.Sprintf("assigned record %d to connection %d", record.Seek, connection.ID)
				oprecord.Send()
				forward(record, connection)
			} else {
				oprecord.Message = fmt.Sprintf("record %v was already enqueued at %d... skipping...", record, record.Enqueued)
				oprecord.Send()
			}
		default:
			runtime.Gosched()
		}
	}
}
