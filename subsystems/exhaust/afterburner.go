package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
)

// Sweeps or requeues records
func afterburner() {
	for {
		record := <-AfterburnerChannel
		if record.Delivered != 0 || !BucketRequired(record.Bucket) {
			if config.Base.Debug {
				log.Print("ab delete bucket ", record.Bucket)
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
