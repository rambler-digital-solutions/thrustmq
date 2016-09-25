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
			fmt.Printf(" | %4d->compressor %4d->combustor %4d->turbine", len(intake.CompressorChannel), len(exhaust.CombustorChannel), len(exhaust.TurbineChannel))
			fmt.Printf(" | conn_id:%d offset:%d", exhaust.State.ConnectionId, exhaust.State.Head)
			oplog.IntakeThroughput = 0
			oplog.ExhaustThroughput = 0
		}
	}
}
