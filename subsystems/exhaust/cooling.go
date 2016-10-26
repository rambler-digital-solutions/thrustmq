package exhaust

import (
	"container/list"
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
	ConnectionsMap[connection.ID] = connection
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
	delete(ConnectionsMap, connection.ID)
	ConnectionsMutex.Unlock()
}

func DeleteConnectionByID(ID uint64) {
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

func BucketRequired(bucketID uint64) bool {
	BucketsMutex.RLock()
	result := BucketsMap[bucketID]
	BucketsMutex.RUnlock()
	if result == nil {
		return false
	}
	return true
}

func RegisterBucketSink(client *common.ConnectionStruct) {
	BucketsMutex.Lock()
	if BucketsMap[client.Bucket] == nil {
		BucketsMap[client.Bucket] = &list.List{}
	}
	client.ListElement = BucketsMap[client.Bucket].PushBack(client)
	BucketsMutex.Unlock()
}

func UnregisterBucketSink(client *common.ConnectionStruct) {
	if client.ListElement == nil {
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
	BucketsMutex.Lock()
	if BucketsMap[bucketID] == nil {
		BucketsMutex.Unlock()
		return nil
	}
	connectionEl := BucketsMap[bucketID].Front()
	BucketsMap[bucketID].MoveToBack(connectionEl)
	connection, _ := connectionEl.Value.(*common.ConnectionStruct)
	BucketsMutex.Unlock()
	return connection
}
