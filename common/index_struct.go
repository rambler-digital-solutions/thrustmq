package common

import (
	"encoding/binary"
	"io"
)

type IndexRecord struct {
	Seek       uint64
	DataSeek   uint64
	DataLength uint64
	BucketId   uint64
	Connection uint64

	Ack byte

	// TODO: replace Ack with explicit:
	// Created uint64
	// Enqueued uint64
	// Sent uint64
	// Delivered uint64
	// Retries uint32
	// could use time.Now().UnixNano() !
}

var IndexSize uint64 = 33

func (self IndexRecord) Serialize() []byte {
	buffer := make([]byte, IndexSize)
	binary.LittleEndian.PutUint64(buffer[0:8], self.DataSeek)
	binary.LittleEndian.PutUint64(buffer[8:16], self.DataLength)
	binary.LittleEndian.PutUint64(buffer[16:24], self.BucketId)
	binary.LittleEndian.PutUint64(buffer[24:32], self.Connection)
	buffer[32] = self.Ack
	return buffer
}

func (self *IndexRecord) Deserialize(reader io.Reader) bool {
	buffer := make([]byte, IndexSize)
	bytesRead, _ := io.ReadFull(reader, buffer)
	if uint64(bytesRead) != IndexSize {
		return false
	}
	self.DataSeek = binary.LittleEndian.Uint64(buffer[0:8])
	self.DataLength = binary.LittleEndian.Uint64(buffer[8:16])
	self.BucketId = binary.LittleEndian.Uint64(buffer[16:24])
	self.Connection = binary.LittleEndian.Uint64(buffer[24:32])
	self.Ack = buffer[32]
	return true
}
