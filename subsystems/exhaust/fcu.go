package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"runtime"
	// "time"
)

// Opens file for reading and adds it to ChunksMap
func getFile(offset uint64) *os.File {
	chunk := common.State.ChunkNumberByOffset(offset)
	file := ChunksMap[chunk]
	if file == nil {
		path := config.Base.IndexPrefix + common.State.StringChunkNumberByOffset(offset)
		file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
		common.FaceIt(err)

		common.Log("fuel", "map chunk %d to %s", chunk, path)

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

	for {
		// rm processed chunks
		for chunkNumber := range ChunksMap {
			if common.ChunkToOffset(int(chunkNumber+1)) <= common.State.UndeliveredOffset {
				common.Log("fuel", "remove chunk #%d (%d >= %d)", chunkNumber, common.State.UndeliveredOffset, common.ChunkToOffset(int(chunkNumber+1)))
				rmFile(common.ChunkToOffset(int(chunkNumber)))
			}
		}
		// process records
		if len(CombustorChannel) < cap(CombustorChannel)/2 {
			start := true
			if common.State.UndeliveredOffset < common.State.WriteOffset {
				common.Log("fuel", "pass %d -> %d", common.State.UndeliveredOffset, common.State.WriteOffset)
			}
			for offset := common.State.UndeliveredOffset; offset < common.State.WriteOffset; offset += common.IndexSize {
				if RecordInMemory(&common.Record{Seek: offset}) {
					continue
				}
				file := getFile(offset)
				if inject(file, offset) {
					start = false
				} else {
					if start {
						common.Log("fuel", "change UndeliveredOffset to %d", offset)
						common.State.UndeliveredOffset = offset + common.IndexSize
					}
				}
			}
		}
		// time.Sleep(1e4)
		runtime.Gosched()
	}
}

func inject(file *os.File, offset uint64) bool {
	_, err := file.Seek(common.State.IndexSeekByOffset(offset), os.SEEK_SET)
	common.FaceIt(err)
	record := &common.Record{}
	record.Deserialize(file)
	record.Seek = offset

	common.Log("fuel", "restore record %d from chunk #%s", record.Seek, common.State.StringChunkNumberByOffset(offset))

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
