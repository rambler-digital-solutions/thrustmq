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
		if record.Delivered != 0 {
			common.Log("afterburner", "deleting delivered record %d from the map", record.Seek)
			DeleteRecord(record)
			continue
		}
		if !BucketRequired(record.Bucket) {
			common.Log("afterburner", "deleting record %d from the map (bucket %d is not required)", record.Seek, record.Bucket)
			DeleteRecord(record)
			continue
		}
		if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
			record.Enqueued = 0
			common.Log("afterburner", "combusting record %d (connection %d is dead)", record.Seek, record.Connection)
			CombustorChannel <- record
		}
	}
}
