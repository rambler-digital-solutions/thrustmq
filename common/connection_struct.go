package common

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
)

type ConnectionStruct struct {
	Connection net.Conn
	Bucket     uint64
	ClientId   uint64
	BatchSize  uint32
	Id         uint64
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	Channel    MessageChannel
}

type ConnectionsMap map[uint64]ConnectionStruct
type BucketsMap map[uint64]([]uint64)

var ConnectionHeaderSize = 20

func (self *ConnectionStruct) DeserializeHeader() {
	buffer := make([]byte, ConnectionHeaderSize)
	_, err := io.ReadFull(self.Reader, buffer)
	FaceIt(err)

	self.ClientId = binary.LittleEndian.Uint64(buffer[0:8])
	self.Bucket = binary.LittleEndian.Uint64(buffer[8:16])
	self.BatchSize = binary.LittleEndian.Uint32(buffer[16:20])
}
