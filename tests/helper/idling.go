package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"time"
)

func WaitForAfterburner() {
	for len(exhaust.AfterburnerChannel) > 0 {
		common.Log("tests", "blocking wait of afterburner...")
		GenericWait()
	}
	GenericWait()
}

func WaitForCombustor() {
	common.Log("tests", "blocking wait of combustor...")
	for len(exhaust.CombustorChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForTurbine() {
	common.Log("tests", "blocking wait of combustor...")
	for len(exhaust.TurbineChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForCompressor() {
	common.Log("tests", "blocking wait of compressor...")
	for len(intake.CompressorStage2Channel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForRecords(amount int) {
	common.Log("tests", "blocking wait of records...")
	for len(exhaust.RecordsMap) < amount {
		GenericWait()
	}
	GenericWait()
}

func WaitForConnectionChannel(id uint64, amount int) {
	common.Log("tests", "blocking wait of connection's #%d channel...", id)
	channel := exhaust.ConnectionsMapGet(id).Channel
	for len(channel) < amount {
		GenericWait()
	}
	GenericWait()
}

func GenericWait() {
	time.Sleep(config.Base.TestDelayDuration)
}
