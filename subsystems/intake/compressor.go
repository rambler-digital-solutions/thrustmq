package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
)

func writeData(file *bufio.Writer, message common.MessageStruct) {
	_, err := file.Write(message.Payload)
	common.FaceIt(err)
}

func writeIndex(file *bufio.Writer, message common.MessageStruct, offset uint64) {
	indexRecord := common.IndexRecord{}
	indexRecord.Offset = offset
	indexRecord.Length = uint64(message.Length)
	indexRecord.BucketId = uint64(message.BucketId)

	file.Write(indexRecord.Serialize())
}

func compressorStage1() {
	for {
		message := <-CompressorChannel
		Stage2CompressorChannel <- message
		select {
		case exhaust.CombustorChannel <- message:
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
	Position := uint64(ptr)
	ptr, err = indexFile.Seek(0, os.SEEK_CUR)
	Offset := uint64(ptr)

	dataWriter := bufio.NewWriterSize(dataFile, config.Base.FileBuffer)
	indexWriter := bufio.NewWriterSize(indexFile, config.Base.FileBuffer)

	for {
		message := <-Stage2CompressorChannel

		writeData(dataWriter, message)
		writeIndex(indexWriter, message, Offset)

		Position += common.IndexSize
		Offset += uint64(message.Length)

		message.AckChannel <- message.PositionInBatch
	}
}
