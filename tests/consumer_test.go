package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"math/rand"
	"testing"
	"time"
	"log"
)

var exhaustInitialized bool = false
var buffer []byte = make([]byte, 1024)

func checkCombustor(t *testing.T, size int) {
	if len(exhaust.CombustorChannel) != size {
		t.Fatalf("combustor channel size %d (should be %d)", len(exhaust.CombustorChannel), size)
	}
}

func checkConnections(t *testing.T, size int) {
	time.Sleep(1e8)
	if len(exhaust.ConnectionsMap) != size {
		t.Fatalf("%d connections instead of %d", len(exhaust.ConnectionsMap), size)
	}
}

func bootstrapExhaust(t *testing.T) {
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		logging.Init()
		exhaust.State.Tail = exhaust.State.Head
		go exhaust.Init()
		time.Sleep(1e8)
		exhaustInitialized = true
	}

	consumer.Disconnect()
	checkConnections(t, 0)
	consumer.Connect()
	checkConnections(t, 1)
}

func TestPing(t *testing.T) {
	consumer.Disconnect()
	log.Println("PING")

	bootstrapExhaust(t)

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
	consumer.Disconnect()
	log.Println("ONE")
	// add pending message with random number
	randomNumber := uint64(rand.Int63())
	binary.LittleEndian.PutUint64(buffer, randomNumber)
	exhaust.CombustorChannel <- common.MessageStruct{Length: 8, Payload: buffer[0:8]}
	checkCombustor(t, 1)

	bootstrapExhaust(t)

	consumer.SendHeader(1, uint64(rand.Uint32()))

	messages := consumer.RecieveBatch()
	checkCombustor(t, 0)

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
	consumer.Disconnect()
	log.Println("MUL")
	batchSize := 3
	randomNumbers := make([]uint64, batchSize)
	for i := 0; i < batchSize; i++ {
		randomNumbers[i] = uint64(rand.Int63())
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, randomNumbers[i])
		exhaust.CombustorChannel <- common.MessageStruct{Length: 8, Payload: payload}
	}

	bootstrapExhaust(t)
	consumer.SendHeader(batchSize, uint64(rand.Int63()))

	log.Println("header!")

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
			t.Fatalf("recieved number %d was not sent at all cat ", actualNumber)
		}
	}
}
