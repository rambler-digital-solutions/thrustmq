package oplog

import (
	"encoding/json"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	Dash = dashboard{
		StartedAt: time.Now(),
		State:     common.State,
		Channels:  &channels{},
		Maps:      &maps{},
		Config:    config.Base}
)

func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	Dash.Channels.Update()
	Dash.Maps.Update()
	json.NewEncoder(w).Encode(Dash)
}

func Init() {
	go logger()
	http.HandleFunc("/dash", dashboardHandler)
	http.ListenAndServe(":3888", nil)
}

func logger() {
	logfile, err := os.OpenFile(config.Base.Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	common.FaceIt(err)
	logger := log.New(logfile, "", log.LstdFlags)
	for {
		logger.Print(<-common.OplogChannel)
	}
}
