package subscriber

import (
	"fmt"
	"net"
)

func Server(filename string, messageBus <-chan string, counter *uint64) {
	fmt.Println("Launching subscriber backend...")

	publisherSocket, _ := net.Listen("tcp", ":2888")

	hash := make(map[net.Conn]chan string)

	go dispatch(filename, messageBus, hash)

	for {
		connection, _ := publisherSocket.Accept()
		inbox := make(chan string, 1000)
		hash[connection] = inbox
		go serve(connection, inbox, counter)
	}
}
