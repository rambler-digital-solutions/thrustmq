package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"time"
)

func WaitForAfterburner() {
	for len(exhaust.AfterburnerChannel) > 0 {
		common.Log("tests", "idling on afterburner...")
		time.Sleep(config.Base.TestDelayDuration)
	}
}

func WaitForCombustor() {
	for len(exhaust.CombustorChannel) > 0 {
		common.Log("tests", "idling on combustor...")
		time.Sleep(config.Base.TestDelayDuration)
	}
}

func WaitForTurbine() {
	for len(exhaust.TurbineChannel) > 0 {
		common.Log("tests", "idling on combustor...")
		time.Sleep(config.Base.TestDelayDuration)
	}
}
