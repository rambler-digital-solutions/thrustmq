package dashboard

import (
	"fmt"
	"thrust/config"
	"thrust/subsystems/exhaust"
	"thrust/subsystems/intake"
	"thrust/subsystems/oplog"
	"time"
)

func Init() {
	for {
		time.Sleep(time.Second)
		if config.Config.Debug {
			fmt.Printf("\r %6d ->msg/sec %6d msg/sec->", oplog.IntakeThroughput, oplog.ExhaustThroughput)
			fmt.Printf(" |  compressor queue [%d] oplog queue [%d] consumers [%d]", len(intake.Channel), len(oplog.Channel), len(exhaust.ConnectionsMap))
			oplog.IntakeThroughput = 0
			oplog.ExhaustThroughput = 0
		}
	}
}
