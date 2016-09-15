package publisher

import (
	"fmt"
	"net"
)

func Server(filename string, updateBus chan<- bool, counter *uint64) {
	fmt.Println("Launching publisher backend...")

	dumperChannel := make(chan string, 1000)

	socket, _ := net.Listen("tcp", ":1888")

	go dump(filename, dumperChannel, updateBus, counter)

	for {
		connection, _ := socket.Accept()
		go serve(connection, dumperChannel)
	}
}
