package config

import (
	"encoding/json"
	"io/ioutil"
	"thrust/common"
)

type exhaustConfigStruct struct {
	Port             int
	TurbineBuffer    int
	CombustionBuffer int
	Chamber          string
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type ConfigStruct struct {
	Intake          intakeConfigStruct
	Exhaust         exhaustConfigStruct
	Data            string
	Index           string
	Debug           bool
	Logfile         string
	IndexRecordSize int64
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
