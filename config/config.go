package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type exhaustConfigStruct struct {
	Port          int
	TurbineBlades int
	RecurringFlux int
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type oplogConfigStruct struct {
	File       string
	BufferSize int
}

type ConfigStruct struct {
	Intake  intakeConfigStruct
	Exhaust exhaustConfigStruct
	Oplog   oplogConfigStruct
	Data    string
	Index   string
	Debug   bool
	Logfile string
}

func loadConfig() ConfigStruct {
	cfg_file := "./config.json"

	raw, err := ioutil.ReadFile(cfg_file)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var config ConfigStruct
	json.Unmarshal(raw, &config)

	return config
}

var Config = loadConfig()
