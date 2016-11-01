package exhaust

import (
	"bufio"
	"fmt"
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
	message := fmt.Sprintf("new consumer #%d %s %s (%d connections)", connectionStruct.ID, address.Network(), address.String(), ConnectionsMapLength())
	common.OplogRecord{Message: message, Subsystem: "exhaust", Action: "newConsumer"}.Send()

	return connectionStruct
}

func registerDisconnect(connectionStruct *common.ConnectionStruct) {
	UnregisterBucketSink(connectionStruct)
	DeleteConnection(connectionStruct)

	address := connectionStruct.Connection.RemoteAddr()
	message := fmt.Sprintf("lost consumer %s %s (%d connections)", address.Network(), address.String(), ConnectionsMapLength())
	common.OplogRecord{Message: message, Subsystem: "exhaust", Action: "lostConsumer"}.Send()
}

func sendBatch(client *common.ConnectionStruct, batch []*common.Record) {
	client.SendActualBatchSize(len(batch))
	for i := 0; i < len(batch); i++ {
		record := <-client.Channel
		record.Sent = common.TimestampUint64()
		err := client.SendMessage(record)
		if err != nil {
			common.OplogRecord{Message: err.Error(), Subsystem: "exhaust"}.Send()
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
			message := fmt.Sprintf("failed ack for %v... returning to combustor", batch[i])
			common.OplogRecord{Message: message, Subsystem: "exhaust"}.Send()
		}
	}
}

func blow(connection net.Conn) {
	client := registerConnect(connection)
	defer registerDisconnect(client)

	if client.DeserializeHeader() {
		RegisterBucketSink(client)
	} else {
		message := fmt.Sprintf("failed to deserialize header for connection %d", client.ID)
		common.OplogRecord{Message: message, Subsystem: "exhaust"}.Send()
		return
	}

	message := fmt.Sprintf("consumer #%d subscribed to bucket %d with batch size %d", client.ID, client.Bucket, client.BatchSize)
	common.Log("exhaust", message)

	time.Sleep(1e6) // allows data to arrive
	for {
		batchSize := common.Min(int(client.BatchSize), len(client.Channel))
		if batchSize > 0 {
			batch := make([]*common.Record, batchSize)
			sendBatch(client, batch)
			receiveAcks(client, batch)
		} else {
			message := fmt.Sprintf("pinging %d", client.ID)
			common.OplogRecord{Message: message, Subsystem: "exhaust"}.Send()
			time.Sleep(1e6)
			runtime.Gosched()
			if !client.Ping() {
				return
			}
		}
	}
}
