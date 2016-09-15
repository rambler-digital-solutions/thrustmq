package logging

import (
  "os"
  "log"
  "net"
  "thrust/config"
)

func Init() (*os.File) {
  logfile, err := os.OpenFile(config.Config.Logfile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if err != nil {
      panic(err)
  }

  log.SetOutput(logfile)
  log.Println("ThrusMQ started")

  return logfile
}

func WatchCapacity(label string, size int, capacity int) {
  if size > capacity * 95 / 100 {
    log.Printf("%s is %d/%d full", label, size, capacity)
  }
}

func NewProducer(address net.Addr) {
  log.Printf("new producer %s %s", address.Network(), address.String())
}

func NewConsumer(address net.Addr) {
  log.Printf("new consumer %s %s", address.Network(), address.String())
}
