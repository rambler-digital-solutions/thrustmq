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
)

var initialized bool = false
var buffer []byte = make([]byte, 1024)

func bootstrap() {
	if !initialized {
		logging.Init()
		go exhaust.Init()
		time.Sleep(1e5)
		consumer.Connect()
		initialized = true
	}
}

func TestPing(t *testing.T) {
	bootstrap()

	consumer.SendHeader(1, 1)

	messages := consumer.RecieveBatch()

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
	bootstrap()

	// add pending message with random number
	randomNumber := uint64(rand.Int63())
	binary.LittleEndian.PutUint64(buffer, randomNumber)
	exhaust.CombustorChannel <- common.MessageStruct{Length: 8, Payload: buffer}

	consumer.SendHeader(1, 1)

	messages := consumer.RecieveBatch()

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
