package logging

import (
	"log"
	"net"
	"os"
	"thrust/common"
	"thrust/config"
)

func Init() *os.File {
	logfile, err := os.OpenFile(config.Config.Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	common.FaceIt(err)

	log.SetOutput(logfile)
	log.Println("ThrusMQ started")

	return logfile
}

func WatchCapacity(label string, size int, capacity int) {
	if size == capacity {
		// log.Printf("%s is %d/%d full", label, size, capacity)
	}
}

func NewProducer(address net.Addr) {
	log.Printf("new producer %s %s", address.Network(), address.String())
}

func LostProducer(address net.Addr) {
	log.Printf("lost producer %s %s", address.Network(), address.String())
}

func NewConsumer(address net.Addr, length int) {
	log.Printf("new consumer %s %s (%d connections)", address.Network(), address.String(), length)
}

func LostConsumer(address net.Addr, length int) {
	log.Printf("lost consumer %s %s (%d connections)", address.Network(), address.String(), length)
}
