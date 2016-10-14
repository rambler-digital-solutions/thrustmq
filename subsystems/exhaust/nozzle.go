package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
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
	connectionStruct.DeserializeHeader()

	ConnectionsMap[connectionStruct.Id] = connectionStruct

	logging.NewConsumer(connectionStruct, len(ConnectionsMap))
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

func blow(connection net.Conn) {
	client := registerConnect(connection)
	defer registerDisconnect(client)

	var batchSize int
	for {
		batchSize = client.NextBatchSize()
		if batchSize > 0 {
			var ackArray []common.MessageStruct = make([]common.MessageStruct, batchSize)

			client.SendActualBatchSize(batchSize)
			for i := 0; i < batchSize; i++ {
				message := <-client.Channel
				err := client.SendMessage(message)
				if err != nil {
					CombustorChannel <- message
					return
				}
				ackArray[i] = message
				oplog.ExhaustThroughput++
			}
			acks, _ := client.GetAcks(batchSize)
			for i := 0; i < batchSize; i++ {
				message := ackArray[i]
				if acks[i] == 1 {
					TurbineChannel <- common.IndexRecord{Connection: client.Id, Position: message.Position, Ack: 2}
				} else {
					CombustorChannel <- message
				}
			}
		} else {
			client.Ping()
			runtime.Gosched()
		}
	}
}
