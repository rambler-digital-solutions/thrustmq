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
)

func registerConnect(connection net.Conn) common.ConnectionStruct {
	State.ConnectionId++

	connectionStruct := common.ConnectionStruct{Connection: connection}
	connectionStruct.Id = State.ConnectionId
	connectionStruct.Reader = bufio.NewReaderSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Writer = bufio.NewWriterSize(connection, config.Base.NetworkBuffer)
	connectionStruct.Channel = make(common.MessageChannel, config.Exhaust.NozzleBuffer)

	ConnectionsMap[connectionStruct.Id] = connectionStruct
	logging.NewConsumer(connectionStruct, len(ConnectionsMap))

	connectionStruct.DeserializeHeader()
	logging.NewConsumerHeader(connectionStruct)

	return connectionStruct
}

func registerDisconnect(connectionStruct common.ConnectionStruct) {
	delete(ConnectionsMap, connectionStruct.Id)
	logging.LostConsumer(connectionStruct.Connection.RemoteAddr(), len(ConnectionsMap))
	for {
		select {
		case CombustorChannel <- <-connectionStruct.Channel:
		default:
			return
		}
	}
}

func sendBatch(client common.ConnectionStruct, batchSize int, ackArray []common.MessageStruct) {
	client.SendActualBatchSize(batchSize)
	for i := 0; i < batchSize; i++ {
		message := <-CombustorChannel
		err := client.SendMessage(message)
		if err != nil {
			CombustorChannel <- message
			return
		}
		ackArray[i] = message
		oplog.ExhaustThroughput++
	}
	client.Writer.Flush()
}

func recieveAcks(client common.ConnectionStruct, batchSize int, ackArray []common.MessageStruct) {
	acks, _ := client.GetAcks(batchSize)
	for i := 0; i < batchSize; i++ {
		message := ackArray[i]
		if acks[i] == 1 {
			record := common.IndexRecord{}
			record.Connection = client.Id
			record.Seek = message.IndexSeek
			record.Delivered = common.TimestampUint64()
			TurbineChannel <- record
		} else {
			CombustorChannel <- message
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
			ackArray := make([]common.MessageStruct, batchSize)
			sendBatch(client, batchSize, ackArray)
			recieveAcks(client, batchSize, ackArray)
		} else {
			log.Printf("Trying to ping client #%d", client.Id)
			client.Ping()
			runtime.Gosched()
		}
	}
}
