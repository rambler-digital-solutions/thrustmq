package intake

import (
	"os"
	"thrust/common"
	"thrust/config"
)

func writeData(file *os.File, message common.MessageStruct) int64 {
	_, err := file.Write(message.Payload)
	common.FaceIt(err)
	offset, _ := file.Seek(0, os.SEEK_CUR)
	return offset - int64(len(message.Payload))
}

func writeIndex(file *os.File, message common.MessageStruct, offset int64) int64 {
	indexRecord := common.IndexRecord{Offset: uint64(offset), Length: uint64(len(message.Payload)), Topic: uint64(message.Topic), Connection: 0, Ack: 0}

	file.Write(indexRecord.Serialize())

	position, _ := file.Seek(0, os.SEEK_CUR)
	return position - common.IndexSize
}

func compressor() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	for {
		message := <-CompressorChannel

		offset := writeData(dataFile, message)
		position := writeIndex(indexFile, message, offset)
		message.Position = position
		message.AckChannel <- true
	}
}
