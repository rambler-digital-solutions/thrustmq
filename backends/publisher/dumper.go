package publisher

import (
	"os"
	"sync/atomic"
)

func dump(filename string, dumperChannel <-chan string, updateBus chan<- bool, counter *uint64) {
	queue, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	for {
		msg := <-dumperChannel
		if _, err := queue.WriteString(msg); err != nil {
			panic(err)
		}
		atomic.AddUint64(counter, 1)
		updateBus <- true
	}
}
