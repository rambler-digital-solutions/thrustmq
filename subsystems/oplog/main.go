package oplog

import (
	"encoding/gob"
	"os"
	"thrust/common"
	"thrust/config"
)

type Record struct {
	Topic     int64
	Subsystem int32
	Operation int32
	Offset    int64
}

var Channel chan Record = make(chan Record, config.Config.Oplog.BufferSize)
var IntakeThroughput int
var ExhaustThroughput int

func Init() {
	file, err := os.OpenFile(config.Config.Oplog.File, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	common.FaceIt(err)

	IntakeThroughput = 0

	enc := gob.NewEncoder(file)
	for {
		oprecord := <-Channel
		err := enc.Encode(oprecord)
		common.FaceIt(err)
		if oprecord.Subsystem == 1 {
			IntakeThroughput += 1
		}
		if oprecord.Subsystem == 2 {
			ExhaustThroughput += 1
		}
	}
}
