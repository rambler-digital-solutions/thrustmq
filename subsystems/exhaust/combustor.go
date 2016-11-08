package exhaust

import (
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
	common.Log("combustor", "forward record %d to connection %d (%d retries)", record.Seek, record.Connection, record.Retries)
	connection.Channel <- record
	common.Log("combustor", "forward record %d to turbine", record.Seek)
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
				if connection == nil {
					common.Log("combustor", "skipping fwd of record %d (connection is nil)", record.Seek)
					continue
				}
				if len(connection.Channel) == cap(connection.Channel) {
					common.Log("combustor", "skipping fwd of record %d (connection %d is full)", record.Seek, connection.ID)
					continue
				}
				common.Log("combustor", "assigned record %d to connection %d", record.Seek, connection.ID)
				forward(record, connection)
			} else {
				common.Log("combustor", "skip record %d (enqueued at %d)", record.Seek, record.Enqueued)
			}
		default:
			runtime.Gosched()
		}
	}
}
