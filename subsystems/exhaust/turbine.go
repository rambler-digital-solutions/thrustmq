package exhaust

import (
	"encoding/gob"
	"io"
	"log"
	"os"
	"thrust/common"
	"thrust/config"
	"time"
)

func turbine() {

	for {
		time.Sleep(1e6)
		markPass()
		fluxPass()
	}
}

func markPass() {
	for {
		if len(TurbineChannel) == 0 {
			return
		}
		<-TurbineChannel
	}
}

func fluxPass() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	dec := gob.NewDecoder(indexFile)
	indexRecord := common.IndexRecord{}

	_, err = indexFile.Seek(State.Head, os.SEEK_SET)
	common.FaceIt(err)

	for {
		err := dec.Decode(&indexRecord)
		if err != nil {
			break
		}

		buffer := make([]byte, indexRecord.Length)
		_, err = dataFile.Seek(int64(indexRecord.Offset), os.SEEK_SET)
		common.FaceIt(err)
		_, err = io.ReadFull(dataFile, buffer)
		common.FaceIt(err)

		position, _ := indexFile.Seek(0, os.SEEK_CUR)
		message := common.MessageStruct{Topic: int64(indexRecord.Topic), Payload: buffer, Position: position}
		State.Head = position
		log.Println(message.Position)
	}
	indexFile.Close()
	dataFile.Close()
}
