package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
)

func DeleteRecord(record *common.Record) {
	RecordsMutex.Lock()
	delete(RecordsMap, record.Seek)
	RecordsMutex.Unlock()
}

func ClearRecordsMap() {
	RecordsMutex.Lock()
	for _, record := range RecordsMap {
		delete(RecordsMap, record.Seek)
	}
	RecordsMutex.Unlock()
}

func MapRecord(record *common.Record) {
	RecordsMutex.Lock()
	RecordsMap[record.Seek] = record
	RecordsMutex.Unlock()
}

func RecordsMapGet(key uint64) *common.Record {
	RecordsMutex.RLock()
	result := RecordsMap[key]
	RecordsMutex.RUnlock()
	return result
}

func RecordInMemory(record *common.Record) bool {
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

func ConnectionsMapLength() int {
	ConnectionsMutex.RLock()
	length := len(ConnectionsMap)
	ConnectionsMutex.RUnlock()
	return length
}

func RecordsMapLength() int {
	RecordsMutex.RLock()
	length := len(RecordsMap)
	RecordsMutex.RUnlock()
	return length
}

func ConnectionsMapGet(key uint64) *common.ConnectionStruct {
	ConnectionsMutex.RLock()
	result := ConnectionsMap[key]
	ConnectionsMutex.RUnlock()
	return result
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

func ConnectionAlive(id uint64) bool {
	ConnectionsMutex.RLock()
	_, ok := ConnectionsMap[id]
	ConnectionsMutex.RUnlock()
	if ok {
		return true
	}
	return false
}

func BucketRequired(bucketId uint64) bool {
	for _, connection := range ConnectionsMap {
		if bucketId == connection.Bucket {
			return true
		}
	}
	return false
}
