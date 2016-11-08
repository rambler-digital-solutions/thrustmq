package exhaust

import (
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

func removeProcessedChunks() {
	for chunkNumber := range ChunksMap {
		if common.ChunkToOffset(int(chunkNumber+1)) <= common.State.UndeliveredOffset {
			common.Log("fuel", "remove chunk #%d (%d >= %d)", chunkNumber, common.State.UndeliveredOffset, common.ChunkToOffset(int(chunkNumber+1)))
			rmFile(common.ChunkToOffset(int(chunkNumber)))
		}
	}
}

// Subsystem that instantiates records from disk and pushes them to combustor
func fuelControlUnit() {
	for {
		time.Sleep(1e3)
		if len(CombustorChannel) > cap(CombustorChannel)/2 || common.State.UndeliveredOffset >= common.State.WriteOffset {
			runtime.Gosched()
			continue
		}
		previousRecordsWereDelivered := true
		jump := common.State.UndeliveredOffset
		floor := common.State.UndeliveredOffset
		ceil := common.State.WriteOffset
		var record *common.Record
		common.Log("fuel", "pass %d -> %d", common.State.UndeliveredOffset, common.State.WriteOffset)
		for offset := floor; offset < ceil; offset += common.IndexSize {
			record = RecordsMapGet(offset)
			if record == nil {
				file := getFile(offset)
				record = inject(file, offset)
			}
			if previousRecordsWereDelivered && record != nil && record.Delivered > 0 {
				if offset != common.State.UndeliveredOffset {
					common.Log("fuel", "change UndeliveredOffset from %d to %d (delivered %d)", jump, offset+common.IndexSize, record.Delivered)
					jump = offset + common.IndexSize
				}
			} else {
				previousRecordsWereDelivered = false
			}
		}
		common.State.UndeliveredOffset = jump
		removeProcessedChunks()
	}
}

func inject(file *os.File, offset uint64) *common.Record {
	_, err := file.Seek(common.State.IndexSeekByOffset(offset), os.SEEK_SET)
	common.FaceIt(err)
	record := &common.Record{}
	record.Seek = offset
	record.Deserialize(file)
	if record.Delivered > 0 {
		// we're not interested in already delivered records =)
		return record
	}

	MapRecord(record)

	dataFile, err := os.OpenFile(config.Base.DataPrefix+common.State.StringChunkNumberByOffset(offset), os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	record.LoadData(dataFile)
	dataFile.Close()

	CombustorChannel <- record
	common.Log("fuel", "restore record %d from chunk #%s (bucket %d)", record.Seek, common.State.StringChunkNumberByOffset(offset), record.Bucket)
	return record
}
