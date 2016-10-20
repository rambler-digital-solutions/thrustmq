package common

import (
	"encoding/binary"
	"io"
)

type IntakeStruct struct {
	AckChannel    chan *IntakeStruct
	NumberInBatch int
	Status        byte
	Record        *Record
}

type IntakeChannel chan *IntakeStruct

var MessageHeaderSize = 12

func (self *IntakeStruct) Deserialize(reader io.Reader) bool {
	header := make([]byte, MessageHeaderSize)
	bytesRead, _ := io.ReadFull(reader, header)
	if bytesRead != MessageHeaderSize {
		return false
	}

	self.Record = &Record{}
	self.Record.Bucket = binary.LittleEndian.Uint64(header[0:8])
	self.Record.DataLength = uint64(binary.LittleEndian.Uint32(header[8:12]))

	buffer := make([]byte, self.Record.DataLength)
	bytesRead, _ = io.ReadFull(reader, buffer)
	if uint64(bytesRead) != self.Record.DataLength {
		return false
	}
	self.Record.Data = buffer
	return true
}
