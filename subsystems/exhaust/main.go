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
	CombustorChannel common.RecordPipe     = make(common.RecordPipe, config.Exhaust.CombustionBuffer)
	ConnectionsMap   common.ConnectionsMap = make(common.ConnectionsMap)
	ConnectionsMutex *sync.RWMutex         = &sync.RWMutex{}
	RecordsMap       common.RecordsMap     = make(common.RecordsMap)
	RecordsMutex     *sync.RWMutex         = &sync.RWMutex{}
)

func DeleteRecord(record *common.Record) {
	RecordsMutex.Lock()
	delete(RecordsMap, record.Seek)
	RecordsMutex.Unlock()
}

func MapRecord(record *common.Record) {
	RecordsMutex.Lock()
	RecordsMap[record.Seek] = record
	RecordsMutex.Unlock()
}

func recordInMemory(record *common.Record) bool {
	RecordsMutex.RLock()
	_, ok := RecordsMap[record.Seek]
	RecordsMutex.RUnlock()
	if ok {
		return true
	}
	return false
}

func MapConnection(connection *common.ConnectionStruct) {
	ConnectionsMutex.Lock()
	ConnectionsMap[connection.Id] = connection
	ConnectionsMutex.Unlock()
}

func DeleteConnection(connection *common.ConnectionStruct) {
	ConnectionsMutex.Lock()
	delete(ConnectionsMap, connection.Id)
	ConnectionsMutex.Unlock()
}

func DeleteConnectionById(id uint64) {
	ConnectionsMutex.Lock()
	delete(ConnectionsMap, id)
	ConnectionsMutex.Unlock()
}

func connectionAlive(id uint64) bool {
	ConnectionsMutex.RLock()
	_, ok := ConnectionsMap[id]
	ConnectionsMutex.RUnlock()
	if ok {
		return true
	}
	return false
}

func BucketRequired(bucketId uint64) bool {
	ConnectionsMutex.RLock()
	for _, connection := range ConnectionsMap {
		if bucketId == connection.Bucket {
			return true
		}
	}
	ConnectionsMutex.RUnlock()
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
