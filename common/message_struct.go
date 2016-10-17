package common

import (
	"encoding/binary"
	"io"
	"os"
)

type MessageChannel chan MessageStruct
type MessageChannels []MessageChannel
type MessageStruct struct {
	AckChannel    chan int
	Payload       []byte
	BucketId      uint64
	Length        uint32
	IndexSeek     uint64
	DataSeek      uint64
	NumberInBatch int
}

var MessageHeaderSize = 12

func (self *MessageStruct) Deserialize(reader io.Reader) bool {
	header := make([]byte, MessageHeaderSize)
	bytesRead, _ := io.ReadFull(reader, header)
	if bytesRead != MessageHeaderSize {
		return false
	}

	self.BucketId = binary.LittleEndian.Uint64(header[0:8])
	self.Length = binary.LittleEndian.Uint32(header[8:12])

	buffer := make([]byte, self.Length)
	bytesRead, _ = io.ReadFull(reader, buffer)
	if uint32(bytesRead) != self.Length {
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

func (self *MessageStruct) Load(file *os.File, record IndexRecord) {
	self.BucketId = record.BucketId
	self.Length = uint32(record.DataLength)
	self.DataSeek = record.DataSeek

	_, err := file.Seek(int64(record.DataSeek), os.SEEK_SET)
	if err != nil {
		return
	}
	self.Payload = make([]byte, self.Length)
	io.ReadFull(file, self.Payload)
}
