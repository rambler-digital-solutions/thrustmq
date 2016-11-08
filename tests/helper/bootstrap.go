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
	SeekStart()
	rand.Seed(time.Now().UTC().UnixNano())
	if intakeInitialized {
		return
	}
	go intake.Init()
	intakeInitialized = true
	LongWait()
}

func BootstrapExhaust(t *testing.T) {
	WaitForAfterburner()
	SeekStart()
	exhaust.ClearRecordsMap()
	ClearCombustor()
	rand.Seed(time.Now().UTC().UnixNano())
	if exhaustInitialized {
		return
	}
	go exhaust.Init()
	exhaustInitialized = true
	LongWait()
}

func SeekStart() {
	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = 0
}

func ReconnectProducer(t *testing.T) {
	producer.Disconnect()
	producer.Connect()
	GenericWait()
}

func ReconnectConsumer(t *testing.T) {
	consumer.Disconnect()
	nextConnId := common.State.ConnectionID + 1
	consumer.Connect()
	WaitForConnectionChannel(nextConnId, 0)
}

func ClearCompressor() {
	for len(intake.CompressorChannel) > 0 {
		<-intake.CompressorChannel
	}
	for len(intake.CompressorStage2Channel) > 0 {
		<-intake.CompressorStage2Channel
	}
}

func ClearCombustor() {
	for len(exhaust.CombustorChannel) > 0 {
		<-exhaust.CombustorChannel
	}
}
