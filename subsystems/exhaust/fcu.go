package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"os"
	"runtime"
	"time"
)

func getFile(offset uint64) *os.File {
	chunk := common.State.ChunkNumberByOffset(offset)
	file := ChunksMap[chunk]
	if file == nil {
		path := config.Base.Index + common.State.StringChunkNumberByOffset(offset)
		file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
		common.FaceIt(err)
		log.Print("FCU maps #", chunk, " to ", path)
		ChunksMap[chunk] = file
		return file
	}
	return file
}

func rmFile(offset uint64) {
	os.Remove(config.Base.Index + common.State.StringChunkNumberByOffset(offset))
	os.Remove(config.Base.Data + common.State.StringChunkNumberByOffset(offset))
	delete(ChunksMap, common.State.ChunkNumberByOffset(offset))
}

func fuelControlUnit() {
	for {
		// rm processed chunks
		for chunkNumber := range ChunksMap {
			if common.ChunkToOffset(int(chunkNumber+1)) <= common.State.MinOffset {
				log.Print("FCU removes #", chunkNumber, " ", common.State.MinOffset, " >= ", common.ChunkToOffset(int(chunkNumber+1)))
				rmFile(common.ChunkToOffset(int(chunkNumber)))
			}
		}
		// process records
		if len(CombustorChannel) < cap(CombustorChannel)/2 {
			start := true
			// log.Print("FCU pass ", common.State.MinOffset, "->",common.State.IndexOffset)
			for offset := common.State.MinOffset; offset < common.State.IndexOffset; offset += common.IndexSize {
				if RecordInMemory(&common.Record{Seek: offset}) {
					continue
				}

				file := getFile(offset)
				if inject(file, offset) {
					start = false
				} else {
					if start {
						log.Print("FCU changes MinOffset to ", offset)
						common.State.MinOffset = offset + common.IndexSize
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
	// log.Print("fcu ", record, " chunk ", common.State.StringChunkNumberByOffset(offset))
	if !RecordInMemory(record) {
		MapRecord(record)
		if record.Delivered == 0 {
			dataFile, err := os.OpenFile(config.Base.Data+common.State.StringChunkNumberByOffset(offset), os.O_RDONLY|os.O_CREATE, 0666)
			common.FaceIt(err)
			record.LoadData(dataFile)
			CombustorChannel <- record
			dataFile.Close()
			return true
		}
	}
	return false
}
