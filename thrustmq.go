package main

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go common.SaveState()

	go intake.Init()
	go exhaust.Init()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	oplog.Init()
}
