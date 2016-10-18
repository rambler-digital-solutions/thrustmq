package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"net"
	"github.com/rambler-digital-solutions/thrustmq/logging"
)

var (
	TurbineChannel                         = make(chan common.IndexRecord, config.Exhaust.TurbineBuffer)
	CombustorChannel common.MessageChannel = make(common.MessageChannel, config.Exhaust.CombustionBuffer)
	ConnectionsMap   common.ConnectionsMap = make(common.ConnectionsMap)
	bucketsMap       common.BucketsMap     = make(common.BucketsMap)
	State            StateStruct           = loadState()
)

func Init() {
	logging.Debug("Init exhaust")

	go saveState()

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	//go combustion()
	//go turbine()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)

		go blow(connection)
	}
}
