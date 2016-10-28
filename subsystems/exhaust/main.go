package exhaust

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"net"
	"os"
	"sync"
)

var (
	CombustorChannel   = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	AfterburnerChannel = make(common.RecordPipe, config.Exhaust.AfterburnerBuffer)
	TurbineChannel     = make(common.RecordPipe, config.Exhaust.TurbineBuffer)
	ConnectionsMap     = make(common.ConnectionsMap)
	RecordsMap         = make(common.RecordsMap)
	BucketsMap         = make(common.BucketsMap)
	ChunksMap          = make(map[uint64]*os.File)
	ConnectionsMutex   = &sync.RWMutex{}
	RecordsMutex       = &sync.RWMutex{}
	BucketsMutex       = &sync.RWMutex{}
)

func Init() {
	logging.Debug("Init exhaust")

	socket, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Exhaust.Port))
	common.FaceIt(err)

	go afterburner()
	go combustor()
	go turbineStage1()
	go turbineStage2()
	go fuelControlUnit()

	var connection net.Conn
	for {
		connection, err = socket.Accept()
		common.FaceIt(err)
		go blow(connection)
	}
}
