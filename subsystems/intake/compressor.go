package intake

import (
	"bufio"
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
	"runtime"
	"time"
)

// first stage of compressor just passes message simultaneously to:
//  the exhaust (delivery)
//  and the second stage of the compressor (storage)
func compressorStage1() {
	for {
		message := <-CompressorChannel
		CompressorStage2Channel <- message
		select {
		case exhaust.CombustorChannel <- message.Record: // combustor is full, do nothing
		default:
			common.Log("compressor", "combustor is full, skipping forward")
		}
	}
}

// Data are stored in chunks, thus we need a switch to a new one when current is full
func nextChunkFile() (*os.File, *os.File, *bufio.Writer, *bufio.Writer) {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+common.State.StringChunkNumber(), os.O_WRONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	_, err = indexFile.Seek(common.State.IndexSeek(), os.SEEK_SET)
	common.FaceIt(err)

	dataFile, err := os.OpenFile(config.Base.DataPrefix+common.State.StringChunkNumber(), os.O_WRONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	ptr, _ := dataFile.Seek(0, os.SEEK_END)
	common.State.DataWriteOffset = uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	return indexFile, dataFile, indexWriter, dataWriter
}

// Flush records to the disk, assign offsets, send acks
func compressorStage2() {
	indexFile, dataFile, indexWriter, dataWriter := nextChunkFile()
	for {
		select {
		case message := <-CompressorStage2Channel:
			if common.State.SwitchChunk() {
				indexWriter.Flush()
				dataWriter.Flush()
				indexFile.Close()
				dataFile.Close()
				indexFile, dataFile, indexWriter, dataWriter = nextChunkFile()

				message := fmt.Sprintf("compressor switched to a new chunk: %d seek: %d dataSeek: %d", common.State.ChunkNumber(), common.State.WriteOffset, common.State.DataWriteOffset)
				common.OplogRecord{Message: message, Subsystem: "compressor"}.Send()
			}
			persistRecord(message.Record, indexWriter, dataWriter)
			common.Log("compressor", fmt.Sprintf("compressed seek: %d dataseek: %d datalength: %d to chunk %d", message.Record.Seek, message.Record.DataSeek, message.Record.DataLength, common.State.ChunkNumber()))
			common.State.NextWriteOffset()
			common.State.DataWriteOffset += message.Record.DataLength
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
	record.Seek = common.State.WriteOffset
	record.DataSeek = common.State.DataWriteOffset
	record.Created = uint64(time.Now().UnixNano())
	_, err = indexWriter.Write(record.Serialize())
	common.FaceIt(err)
}
