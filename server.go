package main

import (
	"thrust/logging"
	"thrust/subsystems/common"
	"thrust/subsystems/exhaust"
	"thrust/subsystems/intake"
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
