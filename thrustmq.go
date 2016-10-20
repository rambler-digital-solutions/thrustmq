package main

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/dashboard"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	logging.Init()

	go common.SaveState()

	go intake.Init()
	go exhaust.Init()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dashboard.Init()
}
