package publisher

import (
	"fmt"
	"net"
	"thrust/config"
)

func Server(updateBus chan<- bool, counter *uint64) {
	fmt.Printf("Spinning compressor on port %d\n", config.Config.Intake.Port)

	turbineChannel := make(chan messageStruct, config.Config.Intake.CompressorBlades)
	socket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Intake.Port))

	go spin(turbineChannel, updateBus, counter)

	for {
		connection, _ := socket.Accept()
		go serve(connection, turbineChannel)
	}
}
