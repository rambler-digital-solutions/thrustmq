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
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type Struct struct {
	Intake        intakeConfigStruct
	Exhaust       exhaustConfigStruct
	StateFile     string
	DataPrefix    string
	IndexPrefix   string
	Logfile       string
	Debug         bool
	FileBuffer    int
	NetworkBuffer int
	ChunkSize     uint64
	MaxChunks     uint64
}

func loadConfig() Struct {
	filename := "./config.json"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		filename = "../config.json"
	}

	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var config Struct
	json.Unmarshal(raw, &config)
	return config
}

var Base = loadConfig()
var Intake = Base.Intake
var Exhaust = Base.Exhaust
