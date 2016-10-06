package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"net"
)

var (
	TurbineChannel                         = make(chan common.IndexRecord, config.Config.Exhaust.TurbineBuffer)
	CombustorChannel common.MessageChannel = make(common.MessageChannel, config.Config.Exhaust.CombustionBuffer)
	ConnectionsMap   common.ConnectionsMap = make(common.ConnectionsMap)
	topicsMap        common.TopicsMap      = make(common.TopicsMap)
	State            StateStruct           = loadState()
)

func Init() {
	go saveState()

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Exhaust.Port))
	common.FaceIt(err)

	go combustion()
	go turbine()

	var (
		connection net.Conn
	)
	for {
		connection, _ = socket.Accept()
		go blow(connection)
	}
}
