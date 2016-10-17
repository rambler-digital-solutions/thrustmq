package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"testing"
	"time"
)

var initialized bool = false

func bootstrap(t *testing.T) {
	if !initialized {
		logging.Init()
		go intake.Init()
		time.Sleep(1e6)
		initialized = true
	}
	producer.Disconnect()
	producer.Connect()
}

func TestSendOneMessage(t *testing.T) {
	bootstrap(t)

	messages := make([]producer.Message, 1)
	messages[0] = producer.Message{}
	producer.SendBatch(messages)
	acks := producer.GetAcks(1)

	expectedAcksLength := 1
	if len(acks) != expectedAcksLength {
		t.Fatalf("got %d acks instead of %d", len(acks), expectedAcksLength)
	}

	if acks[0] != 1 {
		t.Fatalf("ack reports error (code %d)", acks[0])
	}
}
