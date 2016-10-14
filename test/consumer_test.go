package tests

import (
	"encoding/binary"
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"testing"
	"time"
)

func TestSomethingStrange(t *testing.T) {
	logging.Init()

	go exhaust.Init()

	time.Sleep(1e5)

	consumer.Connect()

	// Send header
	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint64(buffer[0:8], 1)
	binary.LittleEndian.PutUint64(buffer[8:16], 1)
	binary.LittleEndian.PutUint32(buffer[16:20], 1)
	consumer.Send(buffer)

	// Recieve ping
	buffer = make([]byte, 8)
	consumer.Recieve(buffer)
	batchSize := binary.LittleEndian.Uint32(buffer[0:4])
	if batchSize != 1 {
		t.Fatal(fmt.Sprintf("batch size is ne to 1 (%d) batchSize", batchSize))
	}
	messageLength := binary.LittleEndian.Uint32(buffer[4:8])
	if messageLength != 0 {
		t.Fatal(fmt.Sprintf("batch size is ne to 0 (%d) messageLength", messageLength))
	}
}
