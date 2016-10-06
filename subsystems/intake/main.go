package intake

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"net"
)

var (
	Stage2CompressorChannel common.MessageChannel = make(common.MessageChannel, config.Config.Intake.CompressorBuffer)
	CompressorChannel       common.MessageChannel = make(common.MessageChannel, config.Config.Intake.CompressorBuffer)
)

func Init() {
	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Intake.Port))
	common.FaceIt(err)

	go compressorStage1()
	go compressorStage2()

	var (
		connection net.Conn
	)

	for {
		connection, err = socket.Accept()
		common.FaceIt(err)
		go suck(connection)
	}
}
