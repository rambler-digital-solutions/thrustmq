package publisher

import (
	"fmt"
	"net"
)

func Server(filename string, messageBus chan<- string, counter *uint64) {
	fmt.Println("Launching publisher backend...")

	dumperChannel := make(chan string, 1000)

	socket, _ := net.Listen("tcp", ":1888")

	go dump(filename, dumperChannel, messageBus, counter)

	for {
		connection, _ := socket.Accept()
		go serve(connection, dumperChannel)
	}
}
