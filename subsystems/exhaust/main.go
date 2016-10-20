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
	RecordsMap       common.RecordsMap     = make(common.RecordsMap)
)

func recordInMemory(record *common.Record) bool {
	if _, ok := ConnectionsMap[record.Seek]; ok {
		return false
	}
	return true
}

func connectionAlive(id uint64) bool {
	if _, ok := ConnectionsMap[id]; ok {
		return false
	}
	return true
}

func bucketRequired(id uint64) bool {
	for _, connection := range ConnectionsMap {
		if id == connection.Bucket {
			return true
		}
	}
	return false
}

func Init() {
	logging.Debug("Init exhaust")

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	go combustor()
	go afterburner()
	go turbine()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)

		go blow(connection)
	}
}
