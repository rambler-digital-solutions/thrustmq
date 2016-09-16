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
	hash := make(map[net.Conn]chan *common.MessageStruct)

	go spin(shaft, hash, mutex)

	for {
		connection, _ := socket.Accept()
		go serve(connection, hash, mutex, counter)
	}
}
