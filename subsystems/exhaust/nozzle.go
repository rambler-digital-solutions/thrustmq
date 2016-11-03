package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"net"
	"runtime"
	"time"
)

func registerConnect(connection net.Conn) *common.ConnectionStruct {
	common.State.ConnectionID++

	connectionStruct := &common.ConnectionStruct{}
	connectionStruct.Connection = connection
	connectionStruct.ID = common.State.ConnectionID
	connectionStruct.Reader = bufio.NewReaderSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Writer = bufio.NewWriterSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Channel = make(common.RecordPipe, config.Exhaust.NozzleBuffer)
	MapConnection(connectionStruct)

	address := connectionStruct.Connection.RemoteAddr()
	common.Log("exhaust", "new consumer #%d %s %s (%d connections)", connectionStruct.ID, address.Network(), address.String(), ConnectionsMapLength())

	return connectionStruct
}

func registerDisconnect(connectionStruct *common.ConnectionStruct) {
	address := connectionStruct.Connection.RemoteAddr()
	common.Log("exhaust", "lost consumer %s %s (%d connections)", address.Network(), address.String(), ConnectionsMapLength())

	UnregisterBucketSink(connectionStruct)
	DeleteConnection(connectionStruct)
}

func sendBatch(client *common.ConnectionStruct, batch []*common.Record) {
	client.SendActualBatchSize(len(batch))
	for i := 0; i < len(batch); i++ {
		record := <-client.Channel
		record.Sent = common.TimestampUint64()
		err := client.SendMessage(record)
		if err != nil {
			common.Log("exhaust", err.Error())
			return
		}
		batch[i] = record
	}
	client.Writer.Flush()
}

func receiveAcks(client *common.ConnectionStruct, batch []*common.Record) {
	acks, _ := client.GetAcks(len(batch))
	for i := 0; i < len(batch); i++ {
		if acks[i] == 1 {
			batch[i].Delivered = common.TimestampUint64()
			batch[i].Dirty = true
			TurbineChannel <- batch[i]
		} else {
			common.Log("exhaust", "failed ack for %v... returning to combustor", batch[i])
		}
	}
}

func blow(connection net.Conn) {
	client := registerConnect(connection)
	defer registerDisconnect(client)

	if client.DeserializeHeader() {
		RegisterBucketSink(client)
	} else {
		common.Log("exhaust", "failed to deserialize header for connection %d", client.ID)
		return
	}

	common.Log("exhaust", "consumer #%d subscribed to bucket %d with batch size %d", client.ID, client.Bucket, client.BatchSize)
	time.Sleep(1e6) // allows data to arrive
	for {
		batchSize := common.Min(int(client.BatchSize), len(client.Channel))
		if batchSize > 0 {
			batch := make([]*common.Record, batchSize)
			sendBatch(client, batch)
			receiveAcks(client, batch)
		} else {
			common.Log("exhaust", "pinging %d", client.ID)
			time.Sleep(1e6)
			runtime.Gosched()
			if !client.Ping() {
				return
			}
		}
	}
}
