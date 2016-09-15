package publisher

import (
	"fmt"
	"net"
	"thrust/config"
)

func Server(updateBus chan<- bool, counter *uint64) {
	fmt.Printf("Launching publisher  backend on port %d\n", config.Config.Publisher.Port)

	dumperChannel := make(chan messageStruct, config.Config.Publisher.DumperCapacity)
	socket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Publisher.Port))

	go dump(dumperChannel, updateBus, counter)

	for {
		connection, _ := socket.Accept()
		go serve(connection, dumperChannel)
	}
}
