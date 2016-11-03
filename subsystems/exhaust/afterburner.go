package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
)

// Sweeps or requeues records
func afterburner() {
	for {
		record := <-AfterburnerChannel
		if RecordsMapGet(record.Seek) == nil {
			continue
		}

		if record.Delivered != 0 || !BucketRequired(record.Bucket) {
			common.Log("afterburner", "deleting record %d from map (bucket %d)", record.Seek, record.Bucket)
			DeleteRecord(record)
		} else {
			if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
				record.Enqueued = 0
				common.Log("afterburner", "combusting record %d (connection %d is dead)", record.Seek, record.Connection)
				CombustorChannel <- record
			}
		}
	}
}
