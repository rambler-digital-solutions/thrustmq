package exhaust

import (
	"fmt"
	"net"
	"thrust/config"
	"thrust/subsystems/common"
)

func Init(shaft <-chan bool, counter *uint64) {
	fmt.Printf("Spinning turbine    on port %d\n", config.Config.Exhaust.Port)

	publisherSocket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Exhaust.Port))

	hash := make(map[net.Conn]chan common.MessageStruct)

	go dispatch(shaft, hash)

	for {
		connection, _ := publisherSocket.Accept()
		go serve(connection, hash, counter)
	}
}
