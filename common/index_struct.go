package common

import (
	"encoding/binary"
	"io"
	"os"
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

func (self *IndexRecord) Slots() []*uint64 {
	return []*uint64{&self.DataSeek, &self.DataLength, &self.BucketId, &self.Connection, &self.Created, &self.Enqueued, &self.Sent, &self.Delivered, &self.Retries}
}

func (self *IndexRecord) Serialize() []byte {
	buffer := make([]byte, IndexSize)
	slots := self.Slots()
	for i := range slots {
		binary.LittleEndian.PutUint64(buffer[i*8:(i+1)*8], *slots[i])
	}
	return buffer
}

func (self *IndexRecord) Deserialize(reader io.Reader) {
	buffer := make([]byte, IndexSize)
	io.ReadFull(reader, buffer)
	slots := self.Slots()
	for i := range slots {
		*slots[i] = binary.LittleEndian.Uint64(buffer[i*8 : (i+1)*8])
	}
}

func (self *IndexRecord) Merge(other *IndexRecord) {
	slots1 := self.Slots()
	slots2 := self.Slots()
	for i := range slots1 {
		*slots1[i] = Max(*slots1[i], *slots2[i])
	}
}

func (self *IndexRecord) ForgeMessage(dataFile *os.File) MessageStruct {
	message := MessageStruct{}

	message.BucketId = self.BucketId
	message.Length = uint32(self.DataLength)
	message.IndexSeek = self.Seek
	message.DataSeek = self.DataSeek
	message.LoadData(dataFile)

	return message
}
