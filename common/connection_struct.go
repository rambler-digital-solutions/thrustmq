package common

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"io"
	"log"
	"net"
)

type ConnectionStruct struct {
	Connection  net.Conn
	Bucket      uint64
	Client      uint64
	BatchSize   uint32
	Id          uint64
	Reader      *bufio.Reader
	Writer      *bufio.Writer
	Channel     RecordPipe
	ListElement *list.Element
}

type ConnectionsMap map[uint64]*ConnectionStruct
type BucketsMap map[uint64]*list.List

var ConnectionHeaderSize = 20

func (self *ConnectionStruct) DeserializeHeader() bool {
	buffer := make([]byte, ConnectionHeaderSize)
	_, err := io.ReadFull(self.Reader, buffer)
	if err != nil {
		return false
	}

	self.Client = binary.LittleEndian.Uint64(buffer[0:8])
	self.Bucket = binary.LittleEndian.Uint64(buffer[8:16])
	self.BatchSize = binary.LittleEndian.Uint32(buffer[16:20])

	return true
}

func (self *ConnectionStruct) SendActualBatchSize(batchSize int) {
	self.Writer.Write(BinUint32(uint32(batchSize)))
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
