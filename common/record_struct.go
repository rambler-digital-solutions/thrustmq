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
	Bucket     uint64
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
type RecordsMap map[uint64]*Record

var IndexSize uint64 = 8 * 9

func (record *Record) Slots() []*uint64 {
	return []*uint64{
		&record.DataSeek,
		&record.DataLength,
		&record.Bucket,
		&record.Connection,
		&record.Created,
		&record.Enqueued,
		&record.Sent,
		&record.Delivered,
		&record.Retries}
}

func (record *Record) Serialize() []byte {
	buffer := make([]byte, IndexSize)
	slots := record.Slots()
	for i := range slots {
		binary.LittleEndian.PutUint64(buffer[i*8:(i+1)*8], *slots[i])
	}
	return buffer
}

func (record *Record) Deserialize(reader io.Reader) {
	buffer := make([]byte, IndexSize)
	io.ReadFull(reader, buffer)
	slots := record.Slots()
	for i := range slots {
		*slots[i] = binary.LittleEndian.Uint64(buffer[i*8 : (i+1)*8])
	}
}

func (record *Record) Merge(other *Record) {
	slots1 := record.Slots()
	slots2 := record.Slots()
	for i := range slots1 {
		*slots1[i] = Max(*slots1[i], *slots2[i])
	}
}

func (record *Record) LoadData(file *os.File) {
	_, err := file.Seek(int64(record.DataSeek), os.SEEK_SET)
	if err != nil {
		return
	}
	record.Data = make([]byte, record.DataLength)
	io.ReadFull(file, record.Data)
}

func (record *Record) NetworkSerialize() []byte {
	buffer := make([]byte, 4+record.DataLength)
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(record.DataLength))
	copy(buffer[4:], record.Data[:])
	return buffer
}
