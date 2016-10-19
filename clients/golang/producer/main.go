package producer

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
)

type Message struct {
	Length  int
	Payload []byte
}

var connection net.Conn = nil

func Connect() {
	conn, err := net.Dial("tcp", "127.0.0.1:1888")
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
	if connection == nil {
		panic("producer is not connected")
	}
	connection.Write(data)
}

func Recieve(buffer []byte) {
	connection.Read(buffer)
}

func SendBatch(messages []Message) {
	lengthBuffer := make([]byte, 4)
	longBuffer := make([]byte, 8)
	binary.LittleEndian.PutUint32(lengthBuffer, uint32(len(messages)))
	Send(lengthBuffer)

	for i := range messages {
		binary.LittleEndian.PutUint64(longBuffer, uint64(rand.Int63()))
		Send(longBuffer)
		binary.LittleEndian.PutUint32(lengthBuffer, uint32(messages[i].Length))
		Send(lengthBuffer)
		Send(messages[i].Payload)
	}
}

func GetAcks(length int) []byte {
	acksBuffer := make([]byte, length)
	Recieve(acksBuffer)
	return acksBuffer
}
