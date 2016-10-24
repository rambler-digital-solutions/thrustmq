package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"testing"
)

func TestPing(t *testing.T) {
	consumer.Disconnect()

	helper.BootstrapExhaust(t)

	consumer.SendHeader(1, uint64(rand.Int63()))

	messages := consumer.RecieveBatch()
	consumer.SendAcks(1)

	expectedBatchSize := 1
	expectedMessageLength := 0
	actualBatchSize := len(messages)
	if actualBatchSize != expectedBatchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", expectedBatchSize, actualBatchSize)
	}
	actualMessageLength := messages[0].Length
	if actualMessageLength != expectedMessageLength {
		t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
	}
}

func TestRecipienceOfSingleMessage(t *testing.T) {
	randomNumber := uint64(rand.Int63())

	helper.BootstrapExhaust(t)

	channel := exhaust.ConnectionsMapGet(common.State.ConnectionId).Channel
	record := &common.Record{DataLength: 8, Data: common.BinUint64(randomNumber)}
	channel <- record
	helper.CheckConnectionChannel(t, common.State.ConnectionId, 1)

	consumer.SendHeader(1, uint64(rand.Uint32()))
	messages := consumer.RecieveBatch()
	consumer.SendAcks(1)

	expectedBatchSize := 1
	expectedMessageLength := 8
	expectedNumber := randomNumber

	actualBatchSize := len(messages)
	if actualBatchSize != expectedBatchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", expectedBatchSize, actualBatchSize)
	}
	actualMessageLength := messages[0].Length
	if actualMessageLength != expectedMessageLength {
		t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
	}
	actualNumber := binary.LittleEndian.Uint64(messages[0].Payload)
	if actualNumber != expectedNumber {
		t.Fatalf("recieved number is ne to sent one %d != %d", expectedNumber, actualNumber)
	}
}

func TestRecipienceOfMultipleMessages(t *testing.T) {
	helper.BootstrapExhaust(t)

	batchSize := 3
	bucketId := uint64(rand.Int63())
	randomNumbers := make([]uint64, batchSize)
	channel := exhaust.ConnectionsMapGet(common.State.ConnectionId).Channel
	if len(channel) != 0 {
		for i := 0; i < len(channel); i++ {
			<-channel
		}
	}
	for i := 0; i < batchSize; i++ {
		randomNumbers[i] = uint64(rand.Int63())
		record := &common.Record{DataLength: 8, Data: common.BinUint64(randomNumbers[i]), Bucket: bucketId}
		channel <- record
	}

	consumer.SendHeader(batchSize, bucketId)
	messages := consumer.RecieveBatch()
	consumer.SendAcks(batchSize)

	expectedBatchSize := batchSize
	expectedMessageLength := 8

	actualBatchSize := len(messages)
	if actualBatchSize != expectedBatchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", expectedBatchSize, actualBatchSize)
	}

	for i := 0; i < batchSize; i++ {
		actualMessageLength := messages[i].Length
		if actualMessageLength != expectedMessageLength {
			t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
		}
		actualNumber := binary.LittleEndian.Uint64(messages[i].Payload)
		if !common.Contains(randomNumbers, actualNumber) {
			t.Fatalf("recieved number %d was not sent at all / step %d / conn %d", actualNumber, i, len(channel))
		}
	}
}
