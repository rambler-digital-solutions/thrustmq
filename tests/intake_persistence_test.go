package tests

import (
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"math/rand"
	"os"
	"testing"
	"time"
)

func bootstrapIntake(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	go intake.Init()
	time.Sleep(1e7)
}


func TestIntakePersistence(t *testing.T) {
	bootstrapIntake(t)
	expectedPayload := rand.Uint32()
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, expectedPayload)
	messages := make([]producer.Message, 1)
	messages[0] = producer.Message{Length: len(buffer), Payload: buffer}

	producer.Connect()
	producer.SendBatch(messages)
	producer.GetAcks(1)

	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDONLY, 0666)
	common.FaceIt(err)
	stat, err := indexFile.Stat()
	indexFile.Seek(stat.Size()-int64(common.IndexSize), os.SEEK_SET)

	record := common.IndexRecord{}
	record.Deserialize(indexFile)

	dataFile, err := os.OpenFile(config.Base.Data, os.O_RDONLY, 0666)
	common.FaceIt(err)
	record.LoadData(dataFile)

	actualPayload := binary.LittleEndian.Uint32(record.Data)
	if actualPayload != expectedPayload {
		t.Fatal("payload mismatch", actualPayload, expectedPayload)
	}
}
