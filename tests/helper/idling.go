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
		common.Log("tests", "(!) BLOCKING wait of afterburner...")
		GenericWait()
	}
	GenericWait()
}

func WaitForCombustor() {
	common.Log("tests", "(!) BLOCKING wait of combustor...")
	for len(exhaust.CombustorChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForTurbine() {
	common.Log("tests", "(!) BLOCKING wait of combustor...")
	for len(exhaust.TurbineChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForCompressor() {
	common.Log("tests", "(!) BLOCKING wait of compressor...")
	for len(intake.CompressorStage2Channel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForRecords(amount int) {
	common.Log("tests", "(!) BLOCKING wait of records...")
	for len(exhaust.RecordsMap) == amount {
		GenericWait()
	}
	GenericWait()
}

func WaitForConnectionChannel(id uint64, amount int) {
	common.Log("tests", "(!) BLOCKING wait of %d records in connection %d channel...", amount, id)
	for exhaust.ConnectionsMapGet(id) == nil {
		GenericWait()
	}
	connection := exhaust.ConnectionsMapGet(id)
	for len(connection.Channel) < amount {
		GenericWait()
	}
	GenericWait()
}

func WaitForConnections(amount int) {
	common.Log("tests", "(!) BLOCKING wait of %d total connections", amount)
	GenericWait()
	for exhaust.ConnectionsMapLength() < amount {
		GenericWait()
	}
}

func GenericWait() {
	time.Sleep(config.Base.TestDelayDuration)
}

func LongWait() {
	time.Sleep(10 * config.Base.TestDelayDuration)
}
