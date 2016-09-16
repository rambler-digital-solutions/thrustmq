package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type exhaustConfigStruct struct {
	Port int
}

type intakeConfigStruct struct {
	Port             int
	CompressorBlades int
}

type ConfigStruct struct {
	Intake   intakeConfigStruct
	Exhaust  exhaustConfigStruct
	Filename string
	Logfile  string
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
