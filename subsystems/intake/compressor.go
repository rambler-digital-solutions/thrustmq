package intake

import (
	"encoding/binary"
	"os"
	"thrust/common"
	"thrust/config"
	"thrust/subsystems/oplog"
)

func writeData(file *os.File, message common.MessageStruct) int64 {
	_, err := file.Write(message.Payload)
	common.FaceIt(err)
	offset, _ := file.Seek(0, os.SEEK_CUR)
	return offset - int64(len(message.Payload))
}

func writeIndex(file *os.File, message common.MessageStruct, position int64) {
	uint32Buffer := make([]byte, 4)
	uint64Buffer := make([]byte, 8)

	binary.LittleEndian.PutUint64(uint64Buffer, message.Topic)
	_, err := file.Write(uint64Buffer)
	common.FaceIt(err)

	binary.LittleEndian.PutUint32(uint32Buffer, uint32(len(message.Payload)))
	_, err = file.Write(uint32Buffer)
	common.FaceIt(err)

	binary.LittleEndian.PutUint64(uint64Buffer, uint64(position))
	_, err = file.Write(uint64Buffer)
	common.FaceIt(err)

	offset, _ := file.Seek(0, os.SEEK_CUR)
	// file.Sync()
	oplog.Channel <- oplog.Record{Topic: message.Topic, Subsystem: 1, Operation: 1, Offset: uint64(offset - 20)}
}

func compressor() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	for {
		message := <-Channel
		offset := writeData(dataFile, message)
		writeIndex(indexFile, message, offset)
		message.AckChannel <- true
	}
}
