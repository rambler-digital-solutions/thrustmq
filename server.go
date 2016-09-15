package main

import (
	"thrust/backends/common"
	"thrust/backends/exhaust"
	"thrust/backends/intake"
	"thrust/logging"
)

func main() {
	logfile := logging.Init()
	defer logfile.Close()

	var incomingCounter uint64
	var outgoingCounter uint64

	shaft := make(chan bool)

	go intake.Init(shaft, &incomingCounter)
	go exhaust.Init(shaft, &outgoingCounter)

	common.Report(&incomingCounter, &outgoingCounter, shaft)
}
