package common

import (
	"github.com/rambler-digital-solutions/thrustmq/config"
)

func OffsetToChunk(offset uint64) int {
	return int(offset / IndexSize / config.Base.ChunkSize)
}

func ChunkToOffset(number int) uint64 {
	return uint64(number) * IndexSize / config.Base.ChunkSize
}
