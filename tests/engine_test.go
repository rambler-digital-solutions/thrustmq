package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	// "github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Send message via golang producer client and make sure that it was stored on disk
func TestIntake(t *testing.T) {
	helper.BootstrapIntake(t)

	expectedPayload := rand.Uint32()
	buffer := common.BinUint32(expectedPayload)
	messages := make([]producer.Message, 1)
	messages[0] = producer.Message{Length: len(buffer), Payload: buffer}

	producer.Connect()
	producer.SendBatch(messages)
	producer.GetAcks(1)

	time.Sleep(1e7)

	offset := common.State.IndexOffset - common.IndexSize
	chunk := common.OffsetToChunkString(offset)
	indexFile, err := os.OpenFile(config.Base.Index+chunk, os.O_RDONLY, 0666)
	common.FaceIt(err)
	_, err = indexFile.Seek(common.OffsetToChunkSeek(offset), os.SEEK_SET)
	common.FaceIt(err)

	record := &common.Record{}
	record.Deserialize(indexFile)
	dataFile, err := os.OpenFile(config.Base.Data+chunk, os.O_RDONLY, 0666)
	common.FaceIt(err)
	record.LoadData(dataFile)

	common.State.Save()
	actualPayload := binary.LittleEndian.Uint32(record.Data)
	if actualPayload != expectedPayload {
		t.Fatalf("payload mismatch! got: %d expected: %d", actualPayload, expectedPayload)
	}
}

// Connect to MQ via golang consumer client and recieve several messages from disk
func TestExhaust(t *testing.T) {

	batchSize := 3
	expectedMessageLength := 8
	bucketID := uint64(rand.Int63())
	randomNumbers := make([]uint64, batchSize)

	records := make([]*common.Record, 0)
	for i := 0; i < batchSize; i++ {
		randomNumbers[i] = uint64(rand.Int63())
		record := &common.Record{}
		record.DataLength = 8
		record.Data = common.BinUint64(randomNumbers[i])
		record.Bucket = bucketID
		records = append(records, record)
	}
	helper.DumpRecords(records)

	helper.BootstrapExhaust(t)
	consumer.SendHeader(batchSize, bucketID)
	messages := consumer.RecieveBatch()
	consumer.SendAcks(batchSize)

	if len(messages) != batchSize {
		t.Fatalf("batch size is expected to be %d (%d instead)", batchSize, len(messages))
	}

	for i := 0; i < batchSize; i++ {
		actualMessageLength := messages[i].Length
		if actualMessageLength != expectedMessageLength {
			t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
		}
		actualNumber := binary.LittleEndian.Uint64(messages[i].Payload)
		if !common.Contains(randomNumbers, actualNumber) {
			t.Fatalf("recieved number %d was not sent at all", actualNumber)
		}
	}
}

// Send message via golang producer client and recieve it via golang consumer client
func TestSystem(t *testing.T) {
}
