package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"math/rand"
	"testing"
	"time"
)

var (
	intakeInitialized  = false
	exhaustInitialized = false
	oplogInitialized   = bootstrapOplog()
)

func bootstrapOplog() bool {
	go oplog.Init()
	return true
}

func BootstrapIntake(t *testing.T) {
	if !intakeInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		go intake.Init()
		time.Sleep(config.Base.TestDelayDuration)
		intakeInitialized = true
	}
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = 0
	producer.Disconnect()
	producer.Connect()
}

func BootstrapExhaust(t *testing.T) {
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		go exhaust.Init()
		time.Sleep(config.Base.TestDelayDuration)
		exhaustInitialized = true
	}

	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = 0
	exhaust.ClearRecordsMap()
	consumer.Disconnect()
	consumer.Connect()
}
