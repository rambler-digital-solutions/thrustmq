package common

import (
	"encoding/gob"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"strconv"
	"time"
)

type StateStruct struct {
	MinOffset    uint64
	MaxOffset    uint64
	IndexOffset  uint64
	Capacity     float32
	ConnectionID uint64
}

var State *StateStruct = loadState()

func loadState() *StateStruct {
	if _, err := os.Stat(config.Exhaust.Chamber); err == nil {
		file, err := os.OpenFile(config.Exhaust.Chamber, os.O_RDONLY|os.O_CREATE, 0666)
		FaceIt(err)
		dec := gob.NewDecoder(file)
		result := &StateStruct{}
		err = dec.Decode(&result)
		FaceIt(err)
		file.Close()
		return result
	}
	return &StateStruct{}
}

func SaveState() {
	for {
		time.Sleep(1e7)
		State.Save()
	}
}

func (self *StateStruct) Save() {
	file, err := os.OpenFile(config.Exhaust.Chamber, os.O_WRONLY|os.O_CREATE, 0666)
	FaceIt(err)
	enc := gob.NewEncoder(file)
	err = enc.Encode(State)
	FaceIt(err)
	file.Sync()
	file.Close()
}

func (self *StateStruct) Distance() uint64 {
	return self.MaxOffset - self.MinOffset
}

func (self *StateStruct) SwitchChunk() bool {
	result := self.SwitchChunkByOffset(self.IndexOffset)
	if result && self.ChunkNumber() >= config.Base.MaxChunks {
		self.IndexOffset = 0
	}
	return result
}

func (self *StateStruct) SwitchChunkByOffset(offset uint64) bool {
	return (offset/IndexSize)%config.Base.ChunkSize == 0
}

func (self *StateStruct) NextIndexOffset() {
	self.IndexOffset += IndexSize
}

func (self *StateStruct) ChunkNumber() uint64 {
	return self.ChunkNumberByOffset(self.IndexOffset)
}

func (self *StateStruct) ChunkNumberByOffset(offset uint64) uint64 {
	return offset / IndexSize / config.Base.ChunkSize
}

func (self *StateStruct) StringChunkNumber() string {
	return strconv.Itoa(int(self.ChunkNumber()))
}

func (self *StateStruct) StringChunkNumberByOffset(offset uint64) string {
	return strconv.Itoa(int(self.ChunkNumberByOffset(offset)))
}

func (self *StateStruct) IndexSeek() int64 {
	return OffsetToChunkSeek(self.IndexOffset)
}

func (self *StateStruct) IndexSeekByOffset(offset uint64) int64 {
	return OffsetToChunkSeek(offset)
}
