package exhaust

import (
	"container/list"
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"net"
	"sync"
)

var (
	CombustorChannel common.RecordPipe     = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	TurbineChannel   common.RecordPipe     = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	ConnectionsMap   common.ConnectionsMap = make(common.ConnectionsMap)
	ConnectionsMutex *sync.RWMutex         = &sync.RWMutex{}
	RecordsMap       common.RecordsMap     = make(common.RecordsMap)
	RecordsMutex     *sync.RWMutex         = &sync.RWMutex{}
	BucketsMap       map[uint64]*list.List = make(map[uint64]*list.List)
	BucketsMutex     *sync.RWMutex         = &sync.RWMutex{}
)

func Init() {
	logging.Debug("Init exhaust")

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	go combustor()
	go afterburner()
	go turbine()
	go turbineStage2()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)

		go blow(connection)
	}
}
