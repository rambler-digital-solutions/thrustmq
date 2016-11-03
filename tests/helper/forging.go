package helper

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"math/rand"
)

func ForgeProducerMessages(number int) []*producer.Message {
	messages := make([]*producer.Message, number)
	for i := 0; i < number; i++ {
		payload := make([]byte, rand.Intn(1024))
		messages[i] = &producer.Message{}
		messages[i].Length = len(payload)
		messages[i].Payload = payload
	}
	return messages
}

func ForgeConnection(connectionID uint64, bucketID uint64) *common.ConnectionStruct {
	connection := &common.ConnectionStruct{}
	connection.ID = connectionID
	connection.Bucket = bucketID
	connection.Channel = make(common.RecordPipe, config.Exhaust.NozzleBuffer)
	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint64(buffer[0:8], uint64(rand.Int63()))
	binary.LittleEndian.PutUint64(buffer[8:16], bucketID)
	binary.LittleEndian.PutUint32(buffer[16:20], uint32(1))
	connection.Reader = bufio.NewReader(bytes.NewReader(buffer))
	exhaust.MapConnection(connection)
	exhaust.RegisterBucketSink(connection)
	return connection
}

func ForgeAndMapRecord(seek uint64, bucketID uint64) *common.Record {
	record := &common.Record{}
	record.Bucket = bucketID
	record.Seek = seek
	exhaust.MapRecord(record)
	return record
}
