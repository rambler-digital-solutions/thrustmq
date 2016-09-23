package intake

import (
	"fmt"
	"net"
	"thrust/common"
	"thrust/config"
)

var Channel common.MessageChannel = make(common.MessageChannel, config.Config.Intake.CompressorBuffer)

func Init() {
	fmt.Printf("Spinning fan on port %d\n", config.Config.Intake.Port)

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Intake.Port))
	common.FaceIt(err)

	go compressor()

	for {
		connection, err := socket.Accept()
		common.FaceIt(err)

		go suck(connection)
	}
}
