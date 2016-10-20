package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"net"
)

var (
	CombustorChannel common.RecordPipe     = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	ConnectionsMap   common.ConnectionsMap = make(common.ConnectionsMap)
	State            common.StateStruct    = loadState()
)

func Init() {
	logging.Debug("Init exhaust")

	go saveState()

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	//go combustion()
	//go afterburner()
	//go turbine()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)

		go blow(connection)
	}
}
