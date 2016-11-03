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
	"runtime"
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
		SeekStart()
		go intake.Init()
		runtime.Gosched()
		time.Sleep(config.Base.TestDelayDuration)
		intakeInitialized = true
	}
}

func BootstrapExhaust(t *testing.T) {
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		SeekStart()
		go exhaust.Init()
		runtime.Gosched()
		time.Sleep(config.Base.TestDelayDuration)
		exhaustInitialized = true
	}
}

func SeekStart() {
	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = 0
}

func ReconnectProducer(t *testing.T) {
	producer.Disconnect()
	producer.Connect()
}

func ReconnectConsumer(t *testing.T) {
	consumer.Disconnect()
	consumer.Connect()
}

func ClearCompressor() {
	for len(intake.CompressorChannel) > 0 {
		<-intake.CompressorChannel
	}
	for len(intake.CompressorStage2Channel) > 0 {
		<-intake.CompressorStage2Channel
	}
}
