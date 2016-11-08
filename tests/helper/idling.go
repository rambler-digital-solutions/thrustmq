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
		common.Log("tests", "idling on afterburner...")
		GenericWait()
	}
	GenericWait()
}

func WaitForCombustor() {
	common.Log("tests", "idling on combustor...")
	for len(exhaust.CombustorChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForTurbine() {
	common.Log("tests", "idling on combustor...")
	for len(exhaust.TurbineChannel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func WaitForCompressor() {
	common.Log("tests", "idling on compressor...")
	for len(intake.CompressorStage2Channel) > 0 {
		GenericWait()
	}
	GenericWait()
}

func GenericWait() {
	time.Sleep(config.Base.TestDelayDuration)
}
