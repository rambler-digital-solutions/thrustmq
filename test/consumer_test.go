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

	consumer.Recieve(buffer)

	expectedBatchSize := 1
	expectedMessageLength := 0

	actualBatchSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
	actualMessageLength := int(binary.LittleEndian.Uint32(buffer[4:12]))

	if actualBatchSize != expectedBatchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", expectedBatchSize, actualBatchSize)
	}

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

	consumer.Recieve(buffer)

	expectedBatchSize := 1
	expectedMessageLength := 8
	expectedNumber := randomNumber

	actualBatchSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
	actualMessageLength := int(binary.LittleEndian.Uint32(buffer[4:12]))
	actualNumber := binary.LittleEndian.Uint64(buffer[8:16])

	if actualBatchSize != expectedBatchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", expectedBatchSize, actualBatchSize)
	}
	if actualMessageLength != expectedMessageLength {
		t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
	}
	if actualNumber != expectedNumber {
		t.Fatalf("recieved number is ne to sent one %d != %d", expectedNumber, actualNumber)
	}
}
