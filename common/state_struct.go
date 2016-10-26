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

var State = loadState()

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

func (state *StateStruct) Save() {
	file, err := os.OpenFile(config.Exhaust.Chamber, os.O_WRONLY|os.O_CREATE, 0666)
	FaceIt(err)
	enc := gob.NewEncoder(file)
	err = enc.Encode(State)
	FaceIt(err)
	file.Sync()
	file.Close()
}

func (state *StateStruct) Distance() uint64 {
	return state.MaxOffset - state.MinOffset
}

func (state *StateStruct) SwitchChunk() bool {
	result := state.SwitchChunkByOffset(state.IndexOffset)
	if result && state.ChunkNumber() >= config.Base.MaxChunks {
		state.IndexOffset = 0
	}
	return result
}

func (state *StateStruct) SwitchChunkByOffset(offset uint64) bool {
	return (offset/IndexSize)%config.Base.ChunkSize == 0
}

func (state *StateStruct) NextIndexOffset() {
	state.IndexOffset += IndexSize
}

func (state *StateStruct) ChunkNumber() uint64 {
	return state.ChunkNumberByOffset(state.IndexOffset)
}

func (state *StateStruct) ChunkNumberByOffset(offset uint64) uint64 {
	return offset / IndexSize / config.Base.ChunkSize
}

func (state *StateStruct) StringChunkNumber() string {
	return strconv.Itoa(int(state.ChunkNumber()))
}

func (state *StateStruct) StringChunkNumberByOffset(offset uint64) string {
	return strconv.Itoa(int(state.ChunkNumberByOffset(offset)))
}

func (state *StateStruct) IndexSeek() int64 {
	return OffsetToChunkSeek(state.IndexOffset)
}

func (state *StateStruct) IndexSeekByOffset(offset uint64) int64 {
	return OffsetToChunkSeek(offset)
}
