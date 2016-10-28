package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
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
		if config.Base.Debug {
			log.Print("FCU maps #", chunk, " to ", path)
		}
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
				if config.Base.Debug {
					log.Print("FCU removes #", chunkNumber, " ", common.State.UndeliveredOffset, " >= ", common.ChunkToOffset(int(chunkNumber+1)))
				}
				rmFile(common.ChunkToOffset(int(chunkNumber)))
			}
		}
		// process records
		if len(CombustorChannel) < cap(CombustorChannel)/2 {
			start := true
			if config.Base.Debug {
				log.Print("FCU pass ", common.State.UndeliveredOffset, "->", common.State.NextWriteOffset)
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
						if config.Base.Debug {
							log.Print("FCU changes UndeliveredOffset to ", offset)
						}
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
	if config.Base.Debug {
		log.Print("fcu ", record, " chunk ", common.State.StringChunkNumberByOffset(offset))
	}
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
