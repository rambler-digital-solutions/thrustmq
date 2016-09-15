package subscriber

import (
	"fmt"
	"net"
)

func Server(filename string, updateBus <-chan bool, counter *uint64) {
	fmt.Println("Launching subscriber backend...")

	publisherSocket, _ := net.Listen("tcp", ":2888")

	hash := make(map[net.Conn]chan string)

	go dispatch(filename, updateBus, hash)

	for {
		connection, _ := publisherSocket.Accept()
		inbox := make(chan string, 1024)
		hash[connection] = inbox
		go serve(connection, inbox, counter)
	}
}
