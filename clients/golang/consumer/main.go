package consumer

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
)

type Message struct {
	Length  int
	Payload []byte
}

var connection net.Conn

func Connect() {
	conn, err := net.Dial("tcp", "127.0.0.1:2888")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	connection = conn
}

func Disconnect() {
	if connection != nil {
		connection.Close()
	}
}

func Send(data []byte) {
	connection.Write(data)
}

func Receive(buffer []byte) {
	connection.Read(buffer)
}

func SendHeader(batchSize int, bucketID uint64) {
	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint64(buffer[0:8], uint64(rand.Int63()))
	binary.LittleEndian.PutUint64(buffer[8:16], bucketID)
	binary.LittleEndian.PutUint32(buffer[16:20], uint32(batchSize))
	Send(buffer)
}

func SendAcks(batchSize int) {
	buffer := make([]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		buffer[i] = 1
	}
	Send(buffer)
}

func ReceiveBatchOrPing() []Message {
	buffer := make([]byte, 4)
	Receive(buffer)
	batchSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
	batch := make([]Message, batchSize)

	for i := 0; i < batchSize; i++ {
		Receive(buffer)
		length := int(binary.LittleEndian.Uint32(buffer[0:4]))
		payload := make([]byte, length)
		Receive(payload)
		batch[i] = Message{Length: length, Payload: payload}
	}

	return batch
}

func ReceiveBatch() []Message {
	batch := ReceiveBatchOrPing()
	for len(batch) == 1 && batch[0].Length == 0 {
		log.Print("CLIENT: got ping")
		SendAcks(1)
		batch = ReceiveBatchOrPing()
	}
	return batch
}
