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

	messageBus := make(chan string, 1000)

	go publisher.Server(filename, messageBus, &incomingCounter)
	go subscriber.Server(filename, messageBus, &outgoingCounter)

	common.Report(&incomingCounter, &outgoingCounter)
}
