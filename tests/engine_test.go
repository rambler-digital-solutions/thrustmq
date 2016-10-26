package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestIntake(t *testing.T) {
	helper.BootstrapIntake(t)

	expectedPayload := rand.Uint32()
	buffer := common.BinUint32(expectedPayload)
	messages := make([]producer.Message, 1)
	messages[0] = producer.Message{Length: len(buffer), Payload: buffer}

	producer.Connect()
	producer.SendBatch(messages)
	producer.GetAcks(1)

	time.Sleep(1e6)

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

func TestExhaust(t *testing.T) {
}

func TestSystem(t *testing.T) {
}
