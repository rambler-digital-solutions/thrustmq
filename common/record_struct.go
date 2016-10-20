package common

import (
	"encoding/binary"
	"io"
	"os"
)

type Record struct {
	Seek       uint64
	DataSeek   uint64
	DataLength uint64
	Data       []byte
	BucketId   uint64
	Connection uint64

	Created   uint64
	Enqueued  uint64
	Sent      uint64
	Delivered uint64
	Retries   uint64

	Dirty bool
}

type RecordPipe chan *Record
type RecordPipes []RecordPipe

var IndexSize uint64 = 8 * 9

func (self *Record) Slots() []*uint64 {
	return []*uint64{&self.DataSeek, &self.DataLength, &self.BucketId, &self.Connection, &self.Created, &self.Enqueued, &self.Sent, &self.Delivered, &self.Retries}
}

func (self *Record) Serialize() []byte {
	buffer := make([]byte, IndexSize)
	slots := self.Slots()
	for i := range slots {
		binary.LittleEndian.PutUint64(buffer[i*8:(i+1)*8], *slots[i])
	}
	return buffer
}

func (self *Record) Deserialize(reader io.Reader) {
	buffer := make([]byte, IndexSize)
	io.ReadFull(reader, buffer)
	slots := self.Slots()
	for i := range slots {
		*slots[i] = binary.LittleEndian.Uint64(buffer[i*8 : (i+1)*8])
	}
}

func (self *Record) Merge(other *Record) {
	slots1 := self.Slots()
	slots2 := self.Slots()
	for i := range slots1 {
		*slots1[i] = Max(*slots1[i], *slots2[i])
	}
}

func (self *Record) LoadData(file *os.File) {
	_, err := file.Seek(int64(self.DataSeek), os.SEEK_SET)
	if err != nil {
		return
	}
	self.Data = make([]byte, self.DataLength)
	io.ReadFull(file, self.Data)
}
