package main

import (
	"thrust/logging"
	"thrust/subsystems/dashboard"
	"thrust/subsystems/exhaust"
	"thrust/subsystems/intake"
	"thrust/subsystems/oplog"
)

func main() {
	logging.Init()

	go oplog.Init()

	go intake.Init()
	go exhaust.Init()

	dashboard.Init()
}
