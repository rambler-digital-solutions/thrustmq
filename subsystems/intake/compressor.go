package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
	"runtime"
)

func writeData(file *bufio.Writer, record *common.IndexRecord) {
	_, err := file.Write(record.Data)
	common.FaceIt(err)
}

func writeIndex(file *bufio.Writer, record *common.IndexRecord, offset uint64) {
	record.DataSeek = offset
	file.Write(record.Serialize())
}

func compressorStage1() {
	for {
		message := <-CompressorChannel
		Stage2CompressorChannel <- message
		select {
		case exhaust.CombustorChannel <- message.Record:
		default:
		}
	}
}

func compressorStage2() {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Base.Data, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	ptr, err := indexFile.Seek(0, os.SEEK_CUR)
	IndexOffset := uint64(ptr)
	ptr, err = indexFile.Seek(0, os.SEEK_CUR)
	DataOffset := uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	for {
		select {
		case message := <-Stage2CompressorChannel:
			writeData(dataWriter, message.Record)
			writeIndex(indexWriter, message.Record, DataOffset)

			IndexOffset += common.IndexSize
			DataOffset += uint64(message.Record.DataLength)

			message.AckChannel <- message.NumberInBatch
		default:
			indexWriter.Flush()
			dataWriter.Flush()
			runtime.Gosched()
		}
	}
}
