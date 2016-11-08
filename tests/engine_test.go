package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"os"
	"testing"
)

// Send message via golang producer client and make sure that it was stored on disk
func TestIntake(t *testing.T) {
	common.Log("test", "\n\nTestIntake")
	helper.BootstrapIntake(t)

	expectedPayload := rand.Uint32()
	buffer := common.BinUint32(expectedPayload)
	messages := make([]*producer.Message, 1)
	messages[0] = &producer.Message{Length: len(buffer), Payload: buffer}

	producer.Connect()
	producer.SendBatch(messages)
	producer.GetAcks(1)

	helper.GenericWait()

	offset := common.State.WriteOffset - common.IndexSize
	chunk := common.OffsetToChunkString(offset)
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+chunk, os.O_RDONLY, 0666)
	common.FaceIt(err)
	_, err = indexFile.Seek(common.OffsetToChunkSeek(offset), os.SEEK_SET)
	common.FaceIt(err)

	record := &common.Record{}
	record.Deserialize(indexFile)
	dataFile, err := os.OpenFile(config.Base.DataPrefix+chunk, os.O_RDONLY, 0666)
	common.FaceIt(err)
	record.LoadData(dataFile)

	common.State.Save()
	actualPayload := binary.LittleEndian.Uint32(record.Data)
	if actualPayload != expectedPayload {
		t.Fatalf("payload mismatch! got: %d expected: %d", actualPayload, expectedPayload)
	}
}

// Connect to MQ via golang consumer client and receive several messages from disk
func TestExhaust(t *testing.T) {
	common.Log("test", "\n\nTestExhaust")

	batchSize := 3
	expectedMessageLength := 8
	bucketID := uint64(rand.Int63())
	randomNumbers := make([]uint64, batchSize)
	records := make([]*common.Record, batchSize)
	for i := 0; i < batchSize; i++ {
		randomNumbers[i] = uint64(rand.Int63())
		records[i] = &common.Record{}
		records[i].Bucket = bucketID
		records[i].DataLength = 8
		records[i].Data = common.BinUint64(randomNumbers[i])
	}
	helper.BootstrapExhaust(t)
	helper.ReconnectConsumer(t)
	helper.DumpRecords(records)

	helper.GenericWait()

	consumer.SendHeader(batchSize, bucketID)
	messages := make([]consumer.Message, 0)
	for len(messages) < batchSize {
		newMessages := consumer.ReceiveBatch()
		consumer.SendAcks(len(newMessages))
		messages = append(messages, newMessages...)
	}

	for i := 0; i < batchSize; i++ {
		actualMessageLength := messages[i].Length
		if actualMessageLength != expectedMessageLength {
			t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
		}
		actualNumber := binary.LittleEndian.Uint64(messages[i].Payload)
		if !common.Contains(randomNumbers, actualNumber) {
			t.Fatalf("received number %d was not sent at all", actualNumber)
		}
	}
}

// Send message via golang producer client and receive it via golang consumer client
func TestSystem(t *testing.T) {
	common.Log("test", "\n\nTestSystem")

	helper.BootstrapIntake(t)
	helper.BootstrapExhaust(t)

	helper.ReconnectProducer(t)
	helper.ReconnectConsumer(t)

	batchSize := 3
	bucketID := uint64(rand.Int63())
	expectedMessageLength := 8

	payloads := make([]uint64, batchSize)
	messages := make([]*producer.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		payloads[i] = uint64(rand.Int63())
		buffer := common.BinUint64(payloads[i])
		messages[i] = &producer.Message{}
		messages[i].Length = len(buffer)
		messages[i].BucketID = bucketID
		messages[i].Payload = buffer
	}

	producer.SendBatch(messages)
	producer.GetAcks(batchSize)
	helper.GenericWait()

	consumer.SendHeader(batchSize, bucketID)
	messagesReceived := make([]consumer.Message, 0)
	for len(messagesReceived) < batchSize {
		newMessages := consumer.ReceiveBatch()
		consumer.SendAcks(len(newMessages))
		messagesReceived = append(messagesReceived, newMessages...)
	}

	for i := 0; i < batchSize; i++ {
		actualMessageLength := messagesReceived[i].Length
		if actualMessageLength != expectedMessageLength {
			t.Fatalf("message length is expected to be %d (%d instead)", expectedMessageLength, actualMessageLength)
		}
		actualNumber := binary.LittleEndian.Uint64(messagesReceived[i].Payload)
		if !common.Contains(payloads, actualNumber) {
			t.Fatalf("received number %d was not sent at all", actualNumber)
		}
	}
}
