package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	IndexOffset uint64
	DataOffset  uint64
)

// first stage of compressor just passes message simultaneously to:
//  the exhaust (delivery)
//  and the second stage of the compressor (storage)
func compressorStage1() {
	for {
		message := <-CompressorChannel
		Stage2CompressorChannel <- message
		select {
		case exhaust.CombustorChannel <- message.Record: // combustor is full, do nothing
		default:
		}
	}
}

// Data are stored in chunks, thus we need a switch to another file now and then
func nextChunkFile() (*bufio.Writer, *bufio.Writer) {
	chunkNumber := common.OffsetToChunk(IndexOffset)

	indexFile, err := os.OpenFile(config.Base.Index+strconv.Itoa(chunkNumber), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Base.Data+strconv.Itoa(chunkNumber), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	ptr, err := indexFile.Seek(0, os.SEEK_END)
	IndexOffset = common.ChunkToOffset(chunkNumber) + uint64(ptr)
	ptr, err = dataFile.Seek(0, os.SEEK_END)
	DataOffset = common.ChunkToOffset(chunkNumber) + uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	return indexWriter, dataWriter
}

// Flush records to the disk, assign offsets, send acks
func compressorStage2() {
	indexWriter, dataWriter := nextChunkFile()

	for {
		select {
		case message := <-Stage2CompressorChannel:
			persistRecord(message.Record, indexWriter, dataWriter)

			message.Status = 1
			message.AckChannel <- message

			IndexOffset += common.IndexSize
			DataOffset += message.Record.DataLength
			if IndexOffset/common.IndexSize%config.Base.ChunkSize == 0 {
				indexWriter, dataWriter = nextChunkFile()
			}
		default:
			// nothing to do. let's flush data to the disk
			indexWriter.Flush()
			dataWriter.Flush()
			runtime.Gosched()
		}
	}
}

// Flush the message to the disk
func persistRecord(record *common.Record, indexWriter *bufio.Writer, dataWriter *bufio.Writer) {
	_, err := dataWriter.Write(record.Data)
	common.FaceIt(err)
	record.Seek = IndexOffset
	record.DataSeek = DataOffset
	record.Created = uint64(time.Now().UnixNano())
	_, err = indexWriter.Write(record.Serialize())
	common.FaceIt(err)
}
