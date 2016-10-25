package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"net"
	"sync"
)

var (
	CombustorChannel   common.RecordPipe     = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	AfterburnerChannel common.RecordPipe     = make(common.RecordPipe, config.Exhaust.AfterburnerBuffer)
	TurbineChannel     common.RecordPipe     = make(common.RecordPipe, config.Exhaust.TurbineBuffer)
	ConnectionsMap     common.ConnectionsMap = make(common.ConnectionsMap)
	RecordsMap         common.RecordsMap     = make(common.RecordsMap)
	BucketsMap         common.BucketsMap     = make(common.BucketsMap)
	ConnectionsMutex   *sync.RWMutex         = &sync.RWMutex{}
	RecordsMutex       *sync.RWMutex         = &sync.RWMutex{}
	BucketsMutex       *sync.RWMutex         = &sync.RWMutex{}
)

func Init() {
	logging.Debug("Init exhaust")

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	go afterburner()
	go combustor()
	go fuelControlUnit()
	go turbine()
	go turbineStage2()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)
		go blow(connection)
	}
}
