package publisher

import (
	"os"
	"sync/atomic"
	"thrust/config"
)

func dump(dumperChannel <-chan string, updateBus chan<- bool, counter *uint64) {
	queue, err := os.OpenFile(config.Config.Filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	for {
		msg := <-dumperChannel
		_, err := queue.WriteString(msg)
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(counter, 1)
		// non-blocking notify of dispatcher
		select {
			case updateBus <- true:
			default:
		}
	}
}
