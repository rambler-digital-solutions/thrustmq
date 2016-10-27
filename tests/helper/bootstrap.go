package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"math/rand"
	"testing"
	"time"
)

var (
	intakeInitialized  = false
	exhaustInitialized = false
)

func BootstrapIntake(t *testing.T) {
	common.State.MinOffset = 0
	common.State.IndexOffset = 0
	if !intakeInitialized {
		logging.Init()
		go intake.Init()
		rand.Seed(time.Now().UTC().UnixNano())
		time.Sleep(1e7)
		intakeInitialized = true
	}
	producer.Disconnect()
	producer.Connect()
}

func BootstrapExhaust(t *testing.T) {
	common.State.MinOffset = 0
	common.State.IndexOffset = 0
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		logging.Init()
		go exhaust.Init()
		time.Sleep(1e7)
		exhaustInitialized = true
	}

	exhaust.ClearRecordsMap()

	consumer.Disconnect()
	CheckConnections(t, 0)
	consumer.Connect()
	CheckConnections(t, 1)
}
