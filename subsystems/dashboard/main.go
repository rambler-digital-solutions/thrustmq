package dashboard

import (
	"fmt"
	"thrust/config"
	"thrust/subsystems/intake"
	"thrust/subsystems/oplog"
	"time"
)

func Init() {
	for {
		time.Sleep(time.Second)
		if config.Config.Debug {
			fmt.Printf("\r %6d ->msg/sec %6d msg/sec-> | compressor: %d oplog: %d", oplog.IntakeThroughput, 0, len(intake.Channel), len(oplog.Channel))
			oplog.IntakeThroughput = 0
		}
	}
}
