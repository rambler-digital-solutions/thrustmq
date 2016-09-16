package exhaust

import (
	"fmt"
	"net"
	"sync"
	"thrust/config"
	"thrust/subsystems/common"
)

func Init(shaft <-chan bool, counter *uint64) {
	fmt.Printf("Spinning turbine    on port %d\n", config.Config.Exhaust.Port)

	socket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Exhaust.Port))

	// maps connections to inboxes
	mutex := &sync.Mutex{}
	nozzles := &common.MessageChannels{}

	go spin(shaft, nozzles, mutex)

	for {
		connection, _ := socket.Accept()
		go thrust(connection, nozzles, mutex, counter)
	}
}
