package exhaust

import (
	"container/list"
	"github.com/rambler-digital-solutions/thrustmq/common"
)

/////////////////////////////////////
// Records Map synchronization
/////////////////////////////////////

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
	return !!ok
}

func RecordInMemoryBySeek(seek uint64) bool {
	RecordsMutex.RLock()
	_, ok := RecordsMap[seek]
	RecordsMutex.RUnlock()
	return !!ok
}

func RecordsMapLength() int {
	RecordsMutex.RLock()
	length := len(RecordsMap)
	RecordsMutex.RUnlock()
	return length
}

/////////////////////////////////////
// Connections Map synchronization
/////////////////////////////////////

// MapConnection adds connection to ConnectionsMap
func MapConnection(connection *common.ConnectionStruct) {
	ConnectionsMutex.Lock()
	ConnectionsMap[connection.ID] = connection
	ConnectionsMutex.Unlock()
}

func ConnectionsMapLength() int {
	ConnectionsMutex.RLock()
	length := len(ConnectionsMap)
	ConnectionsMutex.RUnlock()
	return length
}

func ConnectionsMapGet(key uint64) *common.ConnectionStruct {
	ConnectionsMutex.RLock()
	result := ConnectionsMap[key]
	ConnectionsMutex.RUnlock()
	return result
}

func DeleteConnection(connection *common.ConnectionStruct) {
	UnregisterBucketSink(connection)

	ConnectionsMutex.Lock()
	delete(ConnectionsMap, connection.ID)
	ConnectionsMutex.Unlock()
}

func DeleteConnectionByID(ID uint64) {
	ConnectionsMutex.RLock()
	connection := ConnectionsMap[ID]
	ConnectionsMutex.RUnlock()

	UnregisterBucketSink(connection)

	ConnectionsMutex.Lock()
	delete(ConnectionsMap, ID)
	ConnectionsMutex.Unlock()
}

func ConnectionAlive(ID uint64) bool {
	ConnectionsMutex.RLock()
	_, ok := ConnectionsMap[ID]
	ConnectionsMutex.RUnlock()
	if ok {
		return true
	}
	return false
}

/////////////////////////////////////
// Buckets Map synchronization
/////////////////////////////////////

func BucketRequired(bucketID uint64) bool {
	BucketsMutex.RLock()
	result := BucketsMap[bucketID]
	BucketsMutex.RUnlock()
	if result == nil {
		return false
	}
	return true
}

// RegisterBucketSink adds connection to linked list of connections for this bucket
func RegisterBucketSink(client *common.ConnectionStruct) {
	BucketsMutex.Lock()
	if BucketsMap[client.Bucket] == nil {
		BucketsMap[client.Bucket] = &list.List{}
	}
	client.ListElement = BucketsMap[client.Bucket].PushBack(client)
	BucketsMutex.Unlock()
}

func UnregisterBucketSink(client *common.ConnectionStruct) {
	if client == nil || client.ListElement == nil {
		return
	}
	BucketsMutex.Lock()
	list := BucketsMap[client.Bucket]
	list.Remove(client.ListElement)
	client.ListElement = nil
	if BucketsMap[client.Bucket].Len() == 0 {
		delete(BucketsMap, client.Bucket)
	}
	BucketsMutex.Unlock()
}

func nextConnFor(bucketID uint64) *common.ConnectionStruct {
	BucketsMutex.RLock()
	if BucketsMap[bucketID] == nil {
		BucketsMutex.RUnlock()
		return nil
	}
	connectionEl := BucketsMap[bucketID].Front()
	BucketsMap[bucketID].MoveToBack(connectionEl)
	connection, _ := connectionEl.Value.(*common.ConnectionStruct)
	BucketsMutex.RUnlock()
	return connection
}
