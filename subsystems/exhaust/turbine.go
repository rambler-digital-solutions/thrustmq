package exhaust

import (
	"bufio"
	"os"
	"thrust/common"
	"thrust/config"
	"time"
)

func turbine() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_RDWR|os.O_CREATE, 0666)
	dataFile, err := os.OpenFile(config.Config.Data, os.O_RDWR|os.O_CREATE, 0666)
	defer indexFile.Close()
	defer dataFile.Close()

	for {
		time.Sleep(1e9)
		common.FaceIt(err)
		markPass(indexFile)
		if len(ConnectionsMap) > 0 {
			fluxPass(indexFile, dataFile)
		}
	}
}

func markPass(file *os.File) {
	reader := bufio.NewReader(file)
	for {
		if len(TurbineChannel) == 0 {
			return
		}
		marker := <-TurbineChannel

		_, err := file.Seek(int64(marker.Offset), os.SEEK_SET)
		if err != nil {
			return
		}
		file.Write(marker.Serialize())

		_, err = file.Seek(int64(marker.Offset), os.SEEK_SET)
		if err != nil {
			return
		}
		record := common.IndexRecord{}
		record.Deserialize(reader)
	}
}

func fluxPass(file *os.File, dataFile *os.File) {
	ptr := State.Tail
	_, err := file.Seek(ptr, os.SEEK_SET)
	common.FaceIt(err)
	reader := bufio.NewReader(file)
	total := float32(0)
	marked := float32(0)
	streak := true
	for {
		record := common.IndexRecord{}
		if !record.Deserialize(reader) {
			State.Capacity = 1 - marked/total
			return
		}

		if record.Ack != 0 {
			marked += 1
		} else {
			if _, ok := ConnectionsMap[int64(record.Connection)]; !ok {
				message := common.MessageStruct{}
				message.Load(dataFile, record, ptr)
				CombustorChannel <- message
			} else {
				streak = false
			}
		}

		if streak {
			State.Tail = ptr
		}

		ptr += common.IndexSize
		total += 1
	}
}
