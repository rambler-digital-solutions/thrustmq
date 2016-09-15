package intake

import (
	"os"
	"sync/atomic"
	"thrust/config"
	"thrust/subsystems/common"
)

func spin(turbineChannel <-chan common.MessageStruct, shaft chan<- bool, counter *uint64) {
	queue, err := os.OpenFile(config.Config.Filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	for {
		message := <-turbineChannel
		_, err := queue.Write(message.Payload)
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(counter, 1)
		message.AckChannel <- true
		// non-blocking notify of dispatcher
		select {
		case shaft <- true:
		default:
		}
	}
}
