package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/tests/helper"
	"math/rand"
	"os"
	"strconv"
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

	chunkNumber := common.OffsetToChunk(intake.IndexOffset)

	indexFile, err := os.OpenFile(config.Base.Index+strconv.Itoa(chunkNumber), os.O_RDONLY, 0666)
	common.FaceIt(err)
	indexFile.Seek(int64(intake.IndexOffset-common.IndexSize), os.SEEK_SET)

	record := common.Record{}
	record.Deserialize(indexFile)

	dataFile, err := os.OpenFile(config.Base.Data+strconv.Itoa(chunkNumber), os.O_RDONLY, 0666)
	common.FaceIt(err)
	record.LoadData(dataFile)

	actualPayload := binary.LittleEndian.Uint32(record.Data)
	if actualPayload != expectedPayload {
		t.Fatalf("payload mismatch! got: %d expected: %d", actualPayload, expectedPayload)
	}
}

func TestExhaust(t *testing.T) {
}

func TestSystem(t *testing.T) {
}
