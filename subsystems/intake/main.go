package intake

import (
	"fmt"
	"net"
	"thrust/config"
	"thrust/subsystems/common"
)

func Init(shaft chan<- bool, counter *uint64) {
	fmt.Printf("Spinning compressor on port %d\n", config.Config.Intake.Port)

	turbineChannel := make(chan common.MessageStruct, config.Config.Intake.CompressorBlades)
	socket, _ := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Intake.Port))

	go spin(turbineChannel, shaft, counter)

	for {
		connection, _ := socket.Accept()
		go serve(connection, turbineChannel)
	}
}
