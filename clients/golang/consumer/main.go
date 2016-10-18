package consumer

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"log"
)

type Message struct {
	Length  int
	Payload []byte
}

var connection net.Conn = nil

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
		log.Print("Closing!!!")
		connection.Close()
	}
}

func Send(data []byte) {
	connection.Write(data)
}

func Recieve(buffer []byte) {
	connection.Read(buffer)
}

func SendHeader(batchSize int, bucketId uint64) {
	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint64(buffer[0:8], uint64(rand.Int63()))
	binary.LittleEndian.PutUint64(buffer[8:16], bucketId)
	binary.LittleEndian.PutUint32(buffer[16:20], uint32(batchSize))
	Send(buffer)
}

func SendAcks(batchSize int) {
	buffer := make([]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		buffer[i] = 1
	}
	fmt.Println(buffer)
	Send(buffer)
}

func RecieveBatch() []Message {
	buffer := make([]byte, 4)
	Recieve(buffer)
	batchSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
	batch := make([]Message, batchSize)

	for i := 0; i < batchSize; i++ {
		Recieve(buffer)
		length := int(binary.LittleEndian.Uint32(buffer[0:4]))
		payload := make([]byte, length)
		Recieve(payload)
		batch[i] = Message{Length: length, Payload: payload}
	}

	return batch
}
