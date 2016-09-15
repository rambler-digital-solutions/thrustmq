package subscriber

import (
	"fmt"
	"net"
	"thrust/config"
)

func Server(updateBus <-chan bool, counter *uint64) {
	fmt.Printf("Launching subscriber backend on port %d\n", config.Config.Subscriber.Port)

	publisherSocket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Subscriber.Port))

	hash := make(map[net.Conn]chan string)

	go dispatch(updateBus, hash)

	for {
		connection, _ := publisherSocket.Accept()
		inbox := make(chan string, config.Config.Subscriber.InboxCapacity)
		hash[connection] = inbox
		go serve(connection, inbox, counter)
	}
}
