package intake

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"net"
)

var (
	CompressorStage2Channel = make(common.IntakeChannel, config.Intake.CompressorBuffer)
	CompressorChannel       = make(common.IntakeChannel, config.Intake.CompressorBuffer)
)

func Init() {
	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Intake.Port))
	common.FaceIt(err)

	go compressorStage1()
	go compressorStage2()

	var connection net.Conn

	for {
		connection, err = socket.Accept()
		common.FaceIt(err)
		go suck(connection)
	}
}
