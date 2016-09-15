package main

import (
	"thrust/backends/common"
	"thrust/backends/publisher"
	"thrust/backends/subscriber"
	"thrust/config"
)

func main() {
	var incomingCounter uint64
	var outgoingCounter uint64

	updateBus := make(chan bool, config.Config.UpdateBusCapacity)

	go publisher.Server(updateBus, &incomingCounter)
	go subscriber.Server(updateBus, &outgoingCounter)

	common.Report(&incomingCounter, &outgoingCounter)
}
