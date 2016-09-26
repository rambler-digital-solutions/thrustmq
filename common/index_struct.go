package common

import (
	"encoding/binary"
)

type IndexRecord struct {
	Offset     uint64
	Length     uint64
	Topic      uint64
	Connection uint64
	Ack        byte
}

var size = 33

func (self IndexRecord) Serialize() []byte {
	buffer := make([]byte, size)
	binary.LittleEndian.PutUint64(buffer[0:8], self.Offset)
	binary.LittleEndian.PutUint64(buffer[8:16], self.Length)
	binary.LittleEndian.PutUint64(buffer[16:24], self.Topic)
	binary.LittleEndian.PutUint64(buffer[24:32], self.Connection)
	buffer[32] = self.Ack
	return buffer
}

func (self *IndexRecord) Deserialize(buffer []byte) {
	self.Offset = binary.LittleEndian.Uint64(buffer[0:8])
	self.Length = binary.LittleEndian.Uint64(buffer[8:16])
	self.Topic = binary.LittleEndian.Uint64(buffer[16:24])
	self.Connection = binary.LittleEndian.Uint64(buffer[24:32])
	self.Ack = buffer[32]
}
