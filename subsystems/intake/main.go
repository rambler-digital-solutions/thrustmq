package intake

import (
	"fmt"
	"net"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
)

var stage2CompressorChannel common.MessageChannel = make(common.MessageChannel, config.Config.Intake.CompressorBuffer)
var CompressorChannel common.MessageChannel = make(common.MessageChannel, config.Config.Intake.CompressorBuffer)

func Init() {
	fmt.Printf("Spinning fan on port %d\n", config.Config.Intake.Port)

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Intake.Port))
	common.FaceIt(err)

	go compressorStage1()
	go compressorStage2()

	for {
		connection, err := socket.Accept()
		common.FaceIt(err)

		go suck(connection)
	}
}
