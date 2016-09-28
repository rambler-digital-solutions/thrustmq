package config

import (
	"encoding/json"
	"io/ioutil"
	"github.com/rambler-digital-solutions/thrustmq/common"
)

type exhaustConfigStruct struct {
	Port             int
	TurbineBuffer    int
	CombustionBuffer int
	NozzleBuffer     int
	Chamber          string
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type ConfigStruct struct {
	Intake  intakeConfigStruct
	Exhaust exhaustConfigStruct
	Data    string
	Index   string
	Debug   bool
	Logfile string
}

func loadConfig() ConfigStruct {
	cfg_file := "./config.json"

	raw, err := ioutil.ReadFile(cfg_file)
	common.FaceIt(err)

	var config ConfigStruct
	json.Unmarshal(raw, &config)

	return config
}

var Config = loadConfig()
