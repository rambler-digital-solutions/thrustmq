package exhaust

import (
	"fmt"
	"net"
	"thrust/common"
	"thrust/config"
)

var TurbineChannel = make(chan common.IndexRecord, config.Config.Exhaust.TurbineBuffer)
var CombustorChannel common.MessageChannel = make(common.MessageChannel, config.Config.Exhaust.CombustionBuffer)

var ConnectionsMap common.ConnectionsMap = make(common.ConnectionsMap)
var topicsMap common.TopicsMap = make(common.TopicsMap)

var State StateStruct = loadState()

func Init() {
	fmt.Printf("Spinning turbine on port %d\n", config.Config.Exhaust.Port)
	go saveState()

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Config.Exhaust.Port))
	common.FaceIt(err)

	go combustion()
	go turbine()

	for {
		connection, _ := socket.Accept()
		go blow(connection)
	}
}
