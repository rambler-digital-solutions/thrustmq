package common

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
)

type ConnectionStruct struct {
	Connection net.Conn
	Bucket     uint64
	Client     uint64
	BatchSize  uint32
	Id         uint64
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	Channel    RecordPipe
}

type ConnectionsMap map[uint64]*ConnectionStruct

var ConnectionHeaderSize = 20

func (self *ConnectionStruct) DeserializeHeader() {
	buffer := make([]byte, ConnectionHeaderSize)
	_, err := io.ReadFull(self.Reader, buffer)
	FaceIt(err)

	self.Client = binary.LittleEndian.Uint64(buffer[0:8])
	self.Bucket = binary.LittleEndian.Uint64(buffer[8:16])
	self.BatchSize = binary.LittleEndian.Uint32(buffer[16:20])
}

func (self *ConnectionStruct) SendActualBatchSize(batchSize int) {
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, uint32(batchSize))
	self.Writer.Write(buffer)
}

func (self *ConnectionStruct) SendMessage(record *Record) error {
	bytes := record.NetworkSerialize()
	_, err := self.Writer.Write(bytes)
	return err
}

func (self *ConnectionStruct) GetAcks(batchSize int) ([]byte, error) {
	buffer := make([]byte, batchSize)
	_, err := io.ReadFull(self.Reader, buffer)
	return buffer, err
}

func (self *ConnectionStruct) Ping() bool {
	self.SendActualBatchSize(1)
	message := &Record{}
	self.SendMessage(message)
	self.Writer.Flush()

	acks, err := self.GetAcks(1)
	if err != nil || acks[0] != 1 {
		log.Print("Ping failed")
		return false
	}

	return true
}
