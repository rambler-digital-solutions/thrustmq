package common

import (
	"github.com/rambler-digital-solutions/thrustmq/config"
	"strconv"
)

func OffsetToChunk(offset uint64) int {
	return int(offset / IndexSize / config.Base.ChunkSize)
}

func OffsetToChunkString(offset uint64) string {
	return strconv.Itoa(OffsetToChunk(offset))
}

func OffsetToChunkSeek(offset uint64) int64 {
	return int64(offset % (IndexSize * config.Base.ChunkSize))
}

func ChunkToOffset(number int) uint64 {
	return uint64(number) * IndexSize * config.Base.ChunkSize
}