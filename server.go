package main

import (
	"thrust/backends/common"
	"thrust/backends/publisher"
	"thrust/backends/subscriber"
)

func main() {
	filename := "queue.dat"

	var incomingCounter uint64
	var outgoingCounter uint64

	updateBus := make(chan bool, 1024)

	go publisher.Server(filename, updateBus, &incomingCounter)
	go subscriber.Server(filename, updateBus, &outgoingCounter)

	common.Report(&incomingCounter, &outgoingCounter)
}
