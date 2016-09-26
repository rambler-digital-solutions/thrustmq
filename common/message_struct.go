package common

import (
	"encoding/binary"
	"io"
)

type MessageChannel chan MessageStruct
type MessageChannels []MessageChannel
type MessageStruct struct {
	AckChannel chan bool
	Payload    []byte
	Topic      int64
	Length     int32
	Position   int64
}

var MessageHeaderSize = 12

func (self *MessageStruct) Deserialize(reader io.Reader) bool {
	header := make([]byte, MessageHeaderSize)
	bytesRead, _ := io.ReadFull(reader, header)
	if bytesRead != MessageHeaderSize {
		return false
	}

	self.Topic = int64(binary.LittleEndian.Uint64(header[0:8]))
	self.Length = int32(binary.LittleEndian.Uint32(header[8:12]))

	buffer := make([]byte, self.Length)
	bytesRead, _ = io.ReadFull(reader, buffer)
	if int32(bytesRead) != self.Length {
		return false
	}
	self.Payload = buffer
	return true
}

func (self *MessageStruct) Serialize() []byte {
	buffer := make([]byte, 4+self.Length)
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(self.Length))
	copy(buffer[4:], self.Payload[:])
	return buffer
}
