package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
)

// Sweeps or requeues records
func afterburner() {
	for {
		record := <-AfterburnerChannel
		if record.Delivered != 0 || !BucketRequired(record.Bucket) {
			if config.Base.Debug {
				message := fmt.Sprintf("deleting bucket %d", record.Bucket)
				common.OplogRecord{Message: message, Subsystem: "afterburner"}.Send()
			}
			DeleteRecord(record)
		} else {
			if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
				record.Enqueued = 0
				CombustorChannel <- record
			}
		}
	}
}
