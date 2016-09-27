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
	Tail         uint64
	Head         uint64
	Capacity     float32
	ConnectionId uint64
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

	return StateStruct{Tail: 0, ConnectionId: 1}
}

func saveState() {
	for {
		time.Sleep(1e9)
		file, err := os.OpenFile(config.Config.Exhaust.Chamber, os.O_WRONLY|os.O_CREATE, 0666)
		common.FaceIt(err)
		enc := gob.NewEncoder(file)
		err = enc.Encode(State)
		common.FaceIt(err)
		file.Sync()
		file.Close()
	}
}
