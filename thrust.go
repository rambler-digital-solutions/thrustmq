package main

import (
	"thrust/subsystems/dashboard"
	"thrust/subsystems/intake"
	"thrust/subsystems/oplog"
)

func main() {
	go oplog.Init()
	go intake.Init()

	dashboard.Init()
}
