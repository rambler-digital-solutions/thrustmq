package exhaust

import (
	"bufio"
	"os"
	"thrust/common"
	"thrust/config"
	// "time"
	"log"
)

func turbine() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()
	defer dataFile.Close()

	for {
		markPass(indexFile)
		// if len(ConnectionsMap) > 0 {
		// 	fluxPass(indexFile, dataFile)
		// }
	}
}

func markPass(file *os.File) {
	for {
		marker := <-TurbineChannel

		_, err := file.Seek(int64(marker.Offset), os.SEEK_SET)
		if err != nil {
			return
		}

		file.Write(marker.Serialize())

		if len(TurbineChannel) == 0 {
			return
		}
	}
}

func fluxPass(file *os.File, dataFile *os.File) {
	tail := State.Tail
	stat, err := file.Stat()
	head := stat.Size()
	reader := bufio.NewReader(file)
	total := float32((head - tail) / common.IndexSize)
	marked := float32(0)
	streak := true
	record := common.IndexRecord{}
	_, err = file.Seek(tail, os.SEEK_SET)
	common.FaceIt(err)
	for ptr := State.Tail; ptr < head-common.IndexSize; ptr += common.IndexSize {
		record.Deserialize(reader)
		if record.Ack != 0 {
			marked++
		} else {
			if _, ok := ConnectionsMap[int64(record.Connection)]; !ok {
				log.Println("sent back", record)
				message := common.MessageStruct{}
				message.Load(dataFile, record, ptr)
				CombustorChannel <- message
				record.Ack = 2
				TurbineChannel <- record
			} else {
				streak = false
			}
		}
		if streak {
			State.Tail = ptr
		}
	}
	State.Capacity = 1 - marked/total
}
