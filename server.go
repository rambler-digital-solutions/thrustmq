package main

import (
	"thrust/backends/common"
	"thrust/backends/publisher"
	"thrust/backends/subscriber"
	"thrust/logging"
)

func main() {
	logfile := logging.Init()
	defer logfile.Close()

	var incomingCounter uint64
	var outgoingCounter uint64

	updateBus := make(chan bool)

	go publisher.Server(updateBus, &incomingCounter)
	go subscriber.Server(updateBus, &outgoingCounter)

	common.Report(&incomingCounter, &outgoingCounter, updateBus)
}
