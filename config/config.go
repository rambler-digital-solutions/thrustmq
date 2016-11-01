package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"
)

type exhaustConfigStruct struct {
	Port              int
	TurbineBuffer     int
	CombustionBuffer  int
	NozzleBuffer      int
	AfterburnerBuffer int
}

type intakeConfigStruct struct {
	Port             int
	CompressorBuffer int
}

type Struct struct {
	Intake                 *intakeConfigStruct
	Exhaust                *exhaustConfigStruct
	StateFile              string
	DataPrefix             string
	IndexPrefix            string
	Logfile                string
	Debug                  bool
	FileBuffer             int
	NetworkBuffer          int
	ChunkSize              uint64
	MaxChunks              uint64
	OplogChannelLength     int
	HeartbeatDelay         string
	HeartbeatDelayDuration time.Duration
	TestDelay              string
	TestDelayDuration      time.Duration
}

func loadConfig() *Struct {
	filename := "./config.json"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		filename = "../config.json"
	}

	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var config Struct
	err = json.Unmarshal(raw, &config)
	if err != nil {
		panic(err)
	}

	config.HeartbeatDelayDuration, err = time.ParseDuration(config.HeartbeatDelay)
	if err != nil {
		panic(err)
	}

	config.TestDelayDuration, err = time.ParseDuration(config.TestDelay)
	if err != nil {
		panic(err)
	}

	return &config
}

var Base = loadConfig()
var Intake = Base.Intake
var Exhaust = Base.Exhaust
