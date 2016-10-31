package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
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
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = 0
	if !intakeInitialized {
		go intake.Init()
		rand.Seed(time.Now().UTC().UnixNano())
		time.Sleep(1e7)
		intakeInitialized = true
	}
	producer.Disconnect()
	producer.Connect()
}

func BootstrapExhaust(t *testing.T) {
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = 0
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
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
