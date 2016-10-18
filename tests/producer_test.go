package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"math/rand"
	"testing"
	"time"
)

var intakeInitialized bool = false

func bootstrapIntake(t *testing.T) {
	if !intakeInitialized {
		logging.Init()
		go intake.Init()
		rand.Seed(time.Now().UTC().UnixNano())
		time.Sleep(1e6)
		intakeInitialized = true
	}
	producer.Disconnect()
	producer.Connect()
}

func TestSendOneMessage(t *testing.T) {
	bootstrapIntake(t)

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

func TestSendSeveralMessages(t *testing.T) {
	bootstrapIntake(t)

	numberOfMessages := 3
	messages := make([]producer.Message, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		payload := make([]byte, rand.Intn(1024))
		messages[i] = producer.Message{Length: len(payload), Payload: payload}
	}

	producer.SendBatch(messages)
	acks := producer.GetAcks(numberOfMessages)

	if len(acks) != numberOfMessages {
		t.Fatalf("got %d acks instead of %d", len(acks), numberOfMessages)
	}

	for i := 0; i < numberOfMessages; i++ {
		if acks[i] != 1 {
			t.Fatalf("ack #%d reports error (code %d)", i, acks[0])
		}
	}
}
