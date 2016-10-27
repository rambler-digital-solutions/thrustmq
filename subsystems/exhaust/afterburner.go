package exhaust

import (
// "log"
)

// Sweeps or requeues records
func afterburner() {
	for {
		record := <-AfterburnerChannel
		if record.Delivered != 0 || !BucketRequired(record.Bucket) {
			// log.Print("ab delete bucket ", record.Bucket)
			DeleteRecord(record)
		} else {
			if record.Enqueued > 0 && !ConnectionAlive(record.Connection) {
				record.Enqueued = 0
				CombustorChannel <- record
			}
		}
	}
}
