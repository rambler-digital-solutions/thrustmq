package exhaust

import (
	"bufio"
	"fmt"
	"os"
	"thrust/common"
	"thrust/config"
	"time"
)

func turbine() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_RDWR|os.O_CREATE, 0666)
	defer indexFile.Close()

	for {
		time.Sleep(1e6)
		common.FaceIt(err)
		markPass(indexFile)
		fluxPass(indexFile)
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
		fmt.Println(record)
	}
}

func fluxPass(file *os.File) {
	// do nothing
}
