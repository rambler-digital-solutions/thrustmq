package common

import (
	"fmt"
	"sync/atomic"
	"thrust/config"
	"time"
)

func Report(incomingCounter *uint64, outgoingCounter *uint64, shaft chan<- bool) {
	for {
		time.Sleep(time.Second)
		if config.Config.Debug {
			fmt.Printf("\r %6d ->msg/sec %6d msg/sec-> ", *incomingCounter, *outgoingCounter)
		}
		atomic.StoreUint64(incomingCounter, 0)
		atomic.StoreUint64(outgoingCounter, 0)
	}
}
