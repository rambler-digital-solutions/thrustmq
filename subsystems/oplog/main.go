package oplog

import (
	"encoding/json"
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"net/http"
	"os"
	"strings"
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
	go writeLogs()
	http.HandleFunc("/dash", dashboardHandler)
	http.ListenAndServe(":3888", nil)
}

func getLog(filename string) *log.Logger {
	logfile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	common.FaceIt(err)
	logger := log.New(logfile, "", log.LstdFlags)
	logger.Printf("\n\n\n\n Started.")
	return logger
}

func getLogFromMap(logMap map[string]*log.Logger, subsystem string) *log.Logger {
	value := logMap[subsystem]
	if value == nil {
		filename := fmt.Sprintf("%s_%s.log", config.Base.Logfile, subsystem)
		fmt.Printf(filename)
		logMap[subsystem] = getLog(filename)
	}
	return logMap[subsystem]
}

func writeLogs() {
	logger := getLog(config.Base.Logfile + ".log")
	logMap := make(map[string]*log.Logger)
	var prev = common.OplogRecord{}
	var current common.OplogRecord
	var counter = 1
	for {
		current = <-common.OplogChannel
		if current.Message != prev.Message {
			logger.Printf("[%.4s] %s", strings.ToUpper(current.Subsystem), current.Message)
			getLogFromMap(logMap, current.Subsystem).Printf(current.Message)
			counter = 1
			prev = current
		} else {
			counter++
			if counter == 2 {
				logger.Printf("... repeats")
			}
		}
	}
}
