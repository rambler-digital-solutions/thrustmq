package exhaust

import (
	"fmt"
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
			message := fmt.Sprintf("deleting record from map (seek %d bucket %d)", record.Seek, record.Bucket)
			common.OplogRecord{Message: message, Subsystem: "afterburner"}.Send()

			DeleteRecord(record)
		} else {
			if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
				record.Enqueued = 0
				CombustorChannel <- record
			}
		}
	}
}
