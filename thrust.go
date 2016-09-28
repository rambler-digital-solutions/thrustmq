package main

import (
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/dashboard"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
)

func main() {
	logging.Init()

	go intake.Init()
	go exhaust.Init()

	dashboard.Init()
}
