package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"log"
	"net"
	"runtime"
	"strconv"
	"time"
)

func registerConnect(connection net.Conn) common.ConnectionStruct {
	State.ConnectionId++

	connectionStruct := common.ConnectionStruct{Connection: connection}
	connectionStruct.Id = State.ConnectionId
	connectionStruct.Reader = bufio.NewReaderSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Writer = bufio.NewWriterSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Channel = make(common.RecordPipe, config.Exhaust.NozzleBuffer)

	ConnectionsMap[connectionStruct.Id] = connectionStruct
	logging.NewConsumer(connectionStruct, len(ConnectionsMap))

	connectionStruct.DeserializeHeader()
	logging.NewConsumerHeader(connectionStruct)

	return connectionStruct
}

func registerDisconnect(connectionStruct common.ConnectionStruct) {
	delete(ConnectionsMap, connectionStruct.Id)
	logging.LostConsumer(connectionStruct.Connection.RemoteAddr(), len(ConnectionsMap))
}

func sendBatch(client common.ConnectionStruct, batchSize int, ackArray []common.IndexRecord) {
	client.SendActualBatchSize(batchSize)
	for i := 0; i < batchSize; i++ {
		record := *<-CombustorChannel
		err := client.SendMessage(record)
		if err != nil {
			log.Print(err)
			if record.DataLength > 0 {
				CombustorChannel <- &record
			}
			return
		}
		ackArray[i] = record
		oplog.ExhaustThroughput++
	}
	client.Writer.Flush()
}

func recieveAcks(client common.ConnectionStruct, batchSize int, ackArray []common.IndexRecord) {
	acks, _ := client.GetAcks(batchSize)
	for i := 0; i < batchSize; i++ {
		record := ackArray[i]
		if acks[i] == 1 {
			record.Connection = client.Id
			record.Delivered = common.TimestampUint64()
			TurbineChannel <- &record
		} else {
			log.Print("returning record to combustor")
			log.Print(acks[i])
			CombustorChannel <- &record
		}
	}
}

func blow(connection net.Conn) {
	client := registerConnect(connection)
	defer registerDisconnect(client)

	var batchSize int
	for {
		batchSize = common.Min(int(client.BatchSize), len(CombustorChannel))

		if batchSize > 0 {
			ackArray := make([]common.IndexRecord, batchSize)
			sendBatch(client, batchSize, ackArray)
			recieveAcks(client, batchSize, ackArray)
		} else {
			logging.Debug("Trying to ping client", strconv.FormatInt(int64(client.Id), 4), "...")
			time.Sleep(time.Duration(config.Exhaust.HeartbeatRate) * time.Nanosecond)
			runtime.Gosched()
			if !client.Ping() {
				return
			}
		}
	}
}
