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
	ID          uint64
	Reader      *bufio.Reader
	Writer      *bufio.Writer
	Channel     RecordPipe
	ListElement *list.Element
}

type ConnectionsMap map[uint64]*ConnectionStruct
type BucketsMap map[uint64]*list.List

var ConnectionHeaderSize = 20

func (connection *ConnectionStruct) DeserializeHeader() bool {
	buffer := make([]byte, ConnectionHeaderSize)
	_, err := io.ReadFull(connection.Reader, buffer)
	if err != nil {
		return false
	}

	connection.Client = binary.LittleEndian.Uint64(buffer[0:8])
	connection.Bucket = binary.LittleEndian.Uint64(buffer[8:16])
	connection.BatchSize = binary.LittleEndian.Uint32(buffer[16:20])

	return true
}

func (connection *ConnectionStruct) SendActualBatchSize(batchSize int) {
	connection.Writer.Write(BinUint32(uint32(batchSize)))
}

func (connection *ConnectionStruct) SendMessage(record *Record) error {
	bytes := record.NetworkSerialize()
	_, err := connection.Writer.Write(bytes)
	return err
}

func (connection *ConnectionStruct) GetAcks(batchSize int) ([]byte, error) {
	buffer := make([]byte, batchSize)
	_, err := io.ReadFull(connection.Reader, buffer)
	return buffer, err
}

func (connection *ConnectionStruct) Ping() bool {
	connection.SendActualBatchSize(1)
	connection.SendMessage(&Record{})
	connection.Writer.Flush()

	acks, err := connection.GetAcks(1)
	if err != nil || acks[0] != 1 {
		log.Print("Ping failed. ", err)
		return false
	}

	return true
}
