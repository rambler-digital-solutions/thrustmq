package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
	"time"
)

// Opens file for reading and adds it to ChunksMap
func getFile(offset uint64) *os.File {
	chunk := common.State.ChunkNumberByOffset(offset)
	file := ChunksMap[chunk]
	if file == nil {
		path := config.Base.IndexPrefix + common.State.StringChunkNumberByOffset(offset)
		file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
		common.FaceIt(err)

		message := fmt.Sprintf("map #%d to %s", chunk, path)
		common.OplogRecord{Subsystem: "fuel", Message: message}.Send()

		ChunksMap[chunk] = file
		return file
	}
	return file
}

// Removes delivered files from disk
func rmFile(offset uint64) {
	os.Remove(config.Base.IndexPrefix + common.State.StringChunkNumberByOffset(offset))
	os.Remove(config.Base.DataPrefix + common.State.StringChunkNumberByOffset(offset))
	delete(ChunksMap, common.State.ChunkNumberByOffset(offset))
}

// Subsystem that instantiates records from disk and pushes them to combustor
func fuelControlUnit() {
	oprecord := common.OplogRecord{Subsystem: "fuel"}

	for {
		// rm processed chunks
		for chunkNumber := range ChunksMap {
			if common.ChunkToOffset(int(chunkNumber+1)) <= common.State.UndeliveredOffset {
				oprecord.Message = fmt.Sprintf(
					"remove chunk #%d (%d >= %d)",
					chunkNumber,
					common.State.UndeliveredOffset,
					common.ChunkToOffset(int(chunkNumber+1)))
				oprecord.Send()
				rmFile(common.ChunkToOffset(int(chunkNumber)))
			}
		}
		// process records
		if len(CombustorChannel) < cap(CombustorChannel)/2 {
			start := true
			if common.State.UndeliveredOffset < common.State.NextWriteOffset {
				oprecord.Message = fmt.Sprintf("pass %d -> %d", common.State.UndeliveredOffset, common.State.NextWriteOffset)
				oprecord.Send()
			}
			for offset := common.State.UndeliveredOffset; offset < common.State.NextWriteOffset; offset += common.IndexSize {
				if RecordInMemory(&common.Record{Seek: offset}) {
					continue
				}
				file := getFile(offset)
				if inject(file, offset) {
					start = false
				} else {
					if start {
						oprecord.Message = fmt.Sprintf("change UndeliveredOffset to %d", offset)
						oprecord.Send()
						common.State.UndeliveredOffset = offset + common.IndexSize
					}
				}
			}
		}
		time.Sleep(1e6)
		runtime.Gosched()
	}
}

func inject(file *os.File, offset uint64) bool {
	_, err := file.Seek(common.State.IndexSeekByOffset(offset), os.SEEK_SET)
	common.FaceIt(err)
	record := &common.Record{}
	record.Deserialize(file)
	record.Seek = offset

	message := fmt.Sprintf("restore %v from chunk #%s", record.Seek, common.State.StringChunkNumberByOffset(offset))
	common.OplogRecord{Subsystem: "fuel", Message: message}.Send()

	if !RecordInMemory(record) {
		MapRecord(record)
		if record.Delivered == 0 {
			dataFile, err := os.OpenFile(config.Base.DataPrefix+common.State.StringChunkNumberByOffset(offset), os.O_RDONLY|os.O_CREATE, 0666)
			common.FaceIt(err)
			record.LoadData(dataFile)
			CombustorChannel <- record
			dataFile.Close()
			return true
		}
	}
	return false
}
