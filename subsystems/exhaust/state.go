package exhaust

import (
	"encoding/gob"
	"fmt"
	"os"
	"thrust/common"
	"thrust/config"
	"time"
)

type StateStruct struct {
	ConnectionId int64
	HeadPosition int64
	TailPosition int64
}

func loadState() StateStruct {
	if _, err := os.Stat(config.Config.Exhaust.Chamber); err == nil {
		file, err := os.OpenFile(config.Config.Exhaust.Chamber, os.O_RDONLY|os.O_CREATE, 0666)
		common.FaceIt(err)
		dec := gob.NewDecoder(file)
		result := StateStruct{}
		err = dec.Decode(&result)
		common.FaceIt(err)
		file.Close()
		return result
	} else {
		fmt.Println(err)
	}

	return StateStruct{ConnectionId: 1, HeadPosition: 0, TailPosition: 0}
}

func saveState() {
	for {
		time.Sleep(1e6)
		file, err := os.OpenFile(config.Config.Exhaust.Chamber, os.O_WRONLY|os.O_CREATE, 0666)
		common.FaceIt(err)
		state.ConnectionId += 1
		enc := gob.NewEncoder(file)
		err = enc.Encode(state)
		common.FaceIt(err)
		file.Sync()
		file.Close()
	}
}
