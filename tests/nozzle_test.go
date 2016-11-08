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

// Connect to nozzle and wait for ping message (zero length)
func TestPing(t *testing.T) {
	common.Log("test", "\n\nTestPing")
	helper.BootstrapExhaust(t)
	helper.ReconnectConsumer(t)

	consumer.SendHeader(1, uint64(rand.Int63()))
	messages := consumer.ReceiveBatchOrPing()
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

// Send single message, check that received data matches sent one
func TestRecipienceOfSingleMessage(t *testing.T) {
	common.Log("test", "\n\nTestRecipienceOfSingleMessage")
	helper.BootstrapExhaust(t)
	helper.ReconnectConsumer(t)

	randomNumber := uint64(rand.Int63())
	channel := exhaust.ConnectionsMapGet(common.State.ConnectionID).Channel
	record := &common.Record{}
	record.DataLength = 8
	record.Data = common.BinUint64(randomNumber)
	channel <- record
	helper.CheckConnectionChannel(t, common.State.ConnectionID, 1)

	consumer.SendHeader(1, uint64(rand.Uint32()))
	messages := consumer.ReceiveBatch()
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
		t.Fatalf("received number is ne to sent one %d != %d", expectedNumber, actualNumber)
	}
}

// Send several messages, check that received data matches sent one
func TestRecipienceOfMultipleMessages(t *testing.T) {
	common.Log("test", "\n\nTestRecipienceOfMultipleMessages")
	helper.BootstrapExhaust(t)
	helper.ReconnectConsumer(t)

	batchSize := 3
	bucketID := uint64(rand.Int63())
	randomNumbers := make([]uint64, batchSize)
	channel := exhaust.ConnectionsMapGet(common.State.ConnectionID).Channel

	for i := 0; i < batchSize; i++ {
		randomNumbers[i] = uint64(rand.Int63())
		record := &common.Record{}
		record.DataLength = 8
		record.Data = common.BinUint64(randomNumbers[i])
		record.Bucket = bucketID
		channel <- record
	}

	consumer.SendHeader(batchSize, bucketID)
	messages := consumer.ReceiveBatch()
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
			t.Fatalf("received number %d was not sent at all / step %d / conn %d", actualNumber, i, len(channel))
		}
	}
}
