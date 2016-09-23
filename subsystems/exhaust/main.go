package exhaust

import (
	"fmt"
	"net"
	"thrust/common"
	"thrust/config"
)

var ConnectionsMap common.ConnectionsMap = make(common.ConnectionsMap)
var topicsMap common.TopicsMap = make(common.TopicsMap)

var state StateStruct = loadState()

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
