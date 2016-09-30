package dashboard

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"time"
)

func Init() {
	for {
		time.Sleep(time.Second)
		if config.Config.Debug {
			fmt.Printf("\r %6d ->msg/sec %6d msg/sec->", oplog.IntakeThroughput, oplog.ExhaustThroughput)
			fmt.Printf(" | %4d->cp %4d->cp2 %4d->cb %4d->tb", len(intake.CompressorChannel), len(intake.Stage2CompressorChannel), len(exhaust.CombustorChannel), len(exhaust.TurbineChannel))
			fmt.Printf(" | r %d conn_id: %d", oplog.Requeued, exhaust.State.ConnectionId)
			fmt.Printf(" | h %d t %d span: %d capacity: %.2f", exhaust.State.Head, exhaust.State.Tail, (exhaust.State.Head-exhaust.State.Tail)/uint64(common.IndexSize), exhaust.State.Capacity)
			for _, connectionStruct := range exhaust.ConnectionsMap {
				fmt.Printf("%4d ", len(connectionStruct.Channel))
			}
			fmt.Printf(" | %.2f KPa", float32(oplog.IntakeTotal-oplog.ExhaustTotal)/1000)
			oplog.IntakeTotal += oplog.IntakeThroughput
			oplog.ExhaustTotal += oplog.ExhaustThroughput
			oplog.IntakeThroughput = 0
			oplog.ExhaustThroughput = 0
		}
	}
}
