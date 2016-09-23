package oplog

import (
	"encoding/gob"
	"os"
	"thrust/common"
	"thrust/config"
)

type Record struct {
	Topic     uint64
	Subsystem uint32
	Operation uint32
	Offset    uint64
}

var Channel chan Record = make(chan Record, config.Config.Oplog.BufferSize)
var IntakeThroughput int

func Init() {
	file, err := os.OpenFile(config.Config.Oplog.File, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	IntakeThroughput = 0

	enc := gob.NewEncoder(file)
	for {
		oprecord := <-Channel
		err := enc.Encode(oprecord)
		common.FaceIt(err)
		IntakeThroughput += 1
	}
}
