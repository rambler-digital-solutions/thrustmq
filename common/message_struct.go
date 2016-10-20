package common

import (
	"encoding/binary"
	"io"
)

type MessageStruct struct {
	AckChannel    chan int
	NumberInBatch int
	Record        *IndexRecord
}

type MessageChannel chan *MessageStruct

var MessageHeaderSize = 12

func (self *MessageStruct) Deserialize(reader io.Reader) bool {
	header := make([]byte, MessageHeaderSize)
	bytesRead, _ := io.ReadFull(reader, header)
	if bytesRead != MessageHeaderSize {
		return false
	}

	self.Record = &IndexRecord{}
	self.Record.BucketId = binary.LittleEndian.Uint64(header[0:8])
	self.Record.DataLength = uint64(binary.LittleEndian.Uint32(header[8:12]))

	buffer := make([]byte, self.Record.DataLength)
	bytesRead, _ = io.ReadFull(reader, buffer)
	if uint64(bytesRead) != self.Record.DataLength {
		return false
	}
	self.Record.Data = buffer
	return true
}

func (self *MessageStruct) Serialize() []byte {
	buffer := make([]byte, 4+self.Record.DataLength)
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(self.Record.DataLength))
	copy(buffer[4:], self.Record.Data[:])
	return buffer
}
