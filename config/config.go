package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type SubscriberConfigStruct struct {
	Port          int
	InboxCapacity int
}

type PublisherConfigStruct struct {
	Port           int
	DumperCapacity int
}

type ConfigStruct struct {
	Publisher         PublisherConfigStruct
	Subscriber        SubscriberConfigStruct
	Filename          string
	UpdateBusCapacity int
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
