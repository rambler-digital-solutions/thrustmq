package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type exhaustConfigStruct struct {
	Port              int
	TurbineBuffer     int
	CombustionBuffer  int
	NozzleBuffer      int
	HeartbeatRate     int
	AfterburnerBuffer int
	Chamber           string
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type ConfigStruct struct {
	Intake        intakeConfigStruct
	Exhaust       exhaustConfigStruct
	Data          string
	Index         string
	Debug         bool
	Logfile       string
	FileBuffer    int
	NetworkBuffer int
	ChunkSize     uint64
	MaxChunks     uint64
}

func loadConfig() ConfigStruct {
	filename := "./config.json"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		filename = "../config.json"
	}

	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var config ConfigStruct
	json.Unmarshal(raw, &config)

	return config
}

var Base = loadConfig()
var Intake = Base.Intake
var Exhaust = Base.Exhaust
