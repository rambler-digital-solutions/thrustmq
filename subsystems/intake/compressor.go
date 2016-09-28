package intake

import (
	"os"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
)

func writeData(file *os.File, message common.MessageStruct) int64 {
	_, err := file.Write(message.Payload)
	common.FaceIt(err)
	offset, _ := file.Seek(0, os.SEEK_CUR)
	return offset - int64(len(message.Payload))
}

func writeIndex(file *os.File, message common.MessageStruct, offset int64) uint64 {
	indexRecord := common.IndexRecord{Offset: uint64(offset), Length: uint64(len(message.Payload)), Topic: uint64(message.Topic), Connection: 0, Ack: 0}

	file.Write(indexRecord.Serialize())

	position, _ := file.Seek(0, os.SEEK_CUR)
	return uint64(position) - common.IndexSize
}

var Position uint64 = 0

func compressorStage1() {
	for {
		message := <-CompressorChannel
		stage2CompressorChannel <- message
		select {
		case exhaust.CombustorChannel <- message:
		default:
		}
	}
}

func compressorStage2() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	ptr, err := indexFile.Seek(0, os.SEEK_CUR)
	Position = uint64(ptr)

	for {
		message := <-stage2CompressorChannel
		offset := writeData(dataFile, message)
		writeIndex(indexFile, message, offset)
		message.AckChannel <- true
		Position += common.IndexSize
	}
}
