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

	Created   uint64
	Enqueued  uint64
	Sent      uint64
	Delivered uint64
	Retries   uint64
}

var IndexSize uint64 = 8 * 9

func (self IndexRecord) slots() []*uint64 {
	return []*uint64{&self.DataSeek, &self.DataLength, &self.BucketId, &self.Connection, &self.Created, &self.Enqueued, &self.Sent, &self.Delivered, &self.Retries}
}

func (self IndexRecord) Serialize() []byte {
	buffer := make([]byte, IndexSize)
	slots := self.slots()
	for i := range slots {
		binary.LittleEndian.PutUint64(buffer[i*8:(i+1)*8], *slots[i])
	}
	return buffer
}

func (self *IndexRecord) Deserialize(reader io.Reader) {
	buffer := make([]byte, IndexSize)
	io.ReadFull(reader, buffer)
	slots := self.slots()
	for i := range slots {
		*slots[i] = binary.LittleEndian.Uint64(buffer[i*8 : (i+1)*8])
	}
}
