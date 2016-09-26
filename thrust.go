package main

import (
	"thrust/logging"
	"thrust/subsystems/dashboard"
	"thrust/subsystems/exhaust"
	"thrust/subsystems/intake"
)

func main() {
	logging.Init()

	go intake.Init()
	go exhaust.Init()

	dashboard.Init()
}
