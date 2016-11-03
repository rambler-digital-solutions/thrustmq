package tests

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"testing"
)

// Receive one message and check that everything goes smooth
func TestSendOneMessage(t *testing.T) {
	helper.BootstrapIntake(t)
	helper.ReconnectProducer(t)

	messages := helper.ForgeProducerMessages(1)

	producer.SendBatch(messages)
	acks := producer.GetAcks(len(messages))

	if len(acks) != len(messages) {
		t.Fatalf("got %d acks instead of %d", len(acks), len(messages))
	}

	if acks[0] != 1 {
		t.Fatalf("ack reports error (code %d)", acks[0])
	}
}

// Receive several messages and check that everything goes smooth
func TestSendSeveralMessages(t *testing.T) {
	helper.BootstrapIntake(t)
	helper.ReconnectProducer(t)

	messages := helper.ForgeProducerMessages(3)

	producer.SendBatch(messages)
	acks := producer.GetAcks(len(messages))

	if len(acks) != len(messages) {
		t.Fatalf("got %d acks instead of %d", len(acks), len(messages))
	}
	for i := 0; i < len(messages); i++ {
		if acks[i] != 1 {
			t.Fatalf("ack #%d reports error (code %d)", i, acks[0])
		}
	}
}
