package intake

import (
	"encoding/gob"
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

func writeIndex(file *os.File, message common.MessageStruct, offset int64) {
	indexRecord := common.IndexRecord{Offset: offset, Length: len(message.Payload), Topic: message.Topic, Connection: -1, Ack: 0}
	enc := gob.NewEncoder(file)
	err := enc.Encode(&indexRecord)
	common.FaceIt(err)

	poisiton, _ := file.Seek(0, os.SEEK_CUR)
	oplogRecord := oplog.Record{Topic: message.Topic, Subsystem: 1, Operation: 1, Offset: poisiton}
	oplog.Channel <- oplogRecord
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
