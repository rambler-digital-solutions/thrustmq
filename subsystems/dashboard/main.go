package dashboard

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"log"
	"os"
	"time"
)

func output(message string) {
	if daemon {
		fmt.Printf(message)
	} else {
		log.Printf(message)
	}
}

func sleep() {
	if daemon {
		time.Sleep(time.Second)
	} else {
		time.Sleep(time.Minute)
	}
}

var daemon = os.Getenv("FULLTHRUST") == ""

func Init() {
	for {
		statusLine := ""

		if daemon {
			statusLine += fmt.Sprintf("\r %6d ->msg/sec %6d msg/sec->", oplog.IntakeThroughput, oplog.ExhaustThroughput)
		} else {
			statusLine += fmt.Sprintf("\r %7d ->msg/min %7d msg/min->", oplog.IntakeThroughput, oplog.ExhaustThroughput)
		}
		statusLine += fmt.Sprintf(" | %4d->cp %4d->cp2 %4d->cb %4d->tb", len(intake.CompressorChannel), len(intake.Stage2CompressorChannel), len(exhaust.CombustorChannel), len(exhaust.TurbineChannel))
		statusLine += fmt.Sprintf(" | r %d conn_id: %d", oplog.Requeued, exhaust.State.ConnectionId)
		statusLine += fmt.Sprintf(" | h %d t %d s: %d c: %.2f", exhaust.State.Head, exhaust.State.Tail, (exhaust.State.Head-exhaust.State.Tail)/uint64(common.IndexSize), exhaust.State.Capacity)
		for _, connectionStruct := range exhaust.ConnectionsMap {
			statusLine += fmt.Sprintf("%4d ", len(connectionStruct.Channel))
		}
		statusLine += fmt.Sprintf(" | %.2f KPa", float32(oplog.IntakeTotal-oplog.ExhaustTotal)/1000)

		output(statusLine)

		oplog.IntakeTotal += oplog.IntakeThroughput
		oplog.ExhaustTotal += oplog.ExhaustThroughput
		oplog.IntakeThroughput = 0
		oplog.ExhaustThroughput = 0

		sleep()
	}
}
