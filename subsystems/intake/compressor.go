package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
	"runtime"
)

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

	ptr, err := indexFile.Seek(0, os.SEEK_END)
	indexOffset := uint64(ptr)
	ptr, err = dataFile.Seek(0, os.SEEK_END)
	dataOffset := uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	for {
		select {
		case message := <-Stage2CompressorChannel:
			persistRecord(message.Record, indexOffset, indexWriter, dataOffset, dataWriter)

			message.Status = 1
			message.AckChannel <- message

			indexOffset += common.IndexSize
			dataOffset += message.Record.DataLength
		default:
			indexWriter.Flush()
			dataWriter.Flush()
			runtime.Gosched()
		}
	}
}

func persistRecord(record *common.Record, indexOffset uint64, indexWriter *bufio.Writer, dataOffset uint64, dataWriter *bufio.Writer) {
	record.DataSeek = dataOffset
	_, err := dataWriter.Write(record.Data)
	common.FaceIt(err)

	record.Seek = indexOffset
	record.Created = uint64(time.Now().UnixNano())
	_, err = indexWriter.Write(record.Serialize())
	common.FaceIt(err)

	indexWriter.Flush()
	dataWriter.Flush()
}
