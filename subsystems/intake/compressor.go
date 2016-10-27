package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"log"
	"os"
	"runtime"
	"time"
)

var (
	DataOffset uint64
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

// Data are stored in chunks, thus we need a switch to a new one when current is full
func nextChunkFile() (*os.File, *os.File, *bufio.Writer, *bufio.Writer) {
	indexFile, err := os.OpenFile(config.Base.Index+common.State.StringChunkNumber(), os.O_WRONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	_, err = indexFile.Seek(common.State.IndexSeek(), os.SEEK_SET)
	common.FaceIt(err)

	dataFile, err := os.OpenFile(config.Base.Data+common.State.StringChunkNumber(), os.O_WRONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	ptr, err := dataFile.Seek(0, os.SEEK_END)
	DataOffset = uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	return indexFile, dataFile, indexWriter, dataWriter
}

// Flush records to the disk, assign offsets, send acks
func compressorStage2() {
	indexFile, dataFile, indexWriter, dataWriter := nextChunkFile()
	for {
		select {
		case message := <-Stage2CompressorChannel:
			if common.State.SwitchChunk() {
				indexWriter.Flush()
				dataWriter.Flush()
				indexFile.Close()
				dataFile.Close()
				indexFile, dataFile, indexWriter, dataWriter = nextChunkFile()
				log.Print("compressor switched to a new chunk #", common.State.ChunkNumber(), " seek:", common.State.IndexOffset, " dataSeek:", DataOffset)
			}
			persistRecord(message.Record, indexWriter, dataWriter)
			common.State.NextIndexOffset()
			// log.Print("compressing ", message.Record, " chunk ", common.State.ChunkNumber())
			DataOffset += message.Record.DataLength
			// indexWriter.Flush()
			// dataWriter.Flush()
			if message.AckChannel != nil {
				message.Status = 1
				message.AckChannel <- message
			}
		default:
			// nothing to do. let's flush data to the file
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
	record.Seek = common.State.IndexOffset
	record.DataSeek = DataOffset
	record.Created = uint64(time.Now().UnixNano())
	_, err = indexWriter.Write(record.Serialize())
	common.FaceIt(err)
}
