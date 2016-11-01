package producer

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Message struct {
	Length   int
	Payload  []byte
	BucketID uint64
}

var connection net.Conn

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

func Receive(buffer []byte) {
	connection.Read(buffer)
}

func SendBatch(messages []*Message) {
	lengthBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuffer, uint32(len(messages)))
	Send(lengthBuffer)

	for i := range messages {
		bucketBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(bucketBuffer, messages[i].BucketID)
		Send(bucketBuffer)
		length2Buffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(length2Buffer, uint32(messages[i].Length))
		Send(length2Buffer)
		Send(messages[i].Payload)
	}
}

func GetAcks(length int) []byte {
	acksBuffer := make([]byte, length)
	Receive(acksBuffer)
	return acksBuffer
}
