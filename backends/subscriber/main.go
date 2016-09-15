package subscriber

import (
	"fmt"
	"net"
	"thrust/config"
)

func Server(updateBus <-chan bool, counter *uint64) {
	fmt.Printf("Spinning turbine    on port %d\n", config.Config.Exhaust.Port)

	publisherSocket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Exhaust.Port))

	hash := make(map[net.Conn]chan string)

	go dispatch(updateBus, hash)

	for {
		connection, _ := publisherSocket.Accept()
		go serve(connection, hash, counter)
	}
}
