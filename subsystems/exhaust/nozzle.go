package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"net"
	"time"
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

	blankMessage := common.MessageStruct{}
	blankBytes := blankMessage.Serialize()

	for {
		select {
		case message := <-client.Channel:
			bytes := message.Serialize()

			bytesWritten, err := connection.Write(bytes)
			if err != nil || bytesWritten != len(bytes) {
				CombustorChannel <- message
				return
			}

			bfr := make([]byte, 1)
			bytesRead, err := connection.Read(bfr)
			if err != nil || bytesRead != 1 {
				return
			}

			oplog.ExhaustThroughput++

			TurbineChannel <- common.IndexRecord{Connection: client.Id, Position: message.Position, Ack: 2}
		default:
			bytesWritten, err := connection.Write(blankBytes)
			if err != nil || bytesWritten != len(blankBytes) {
				return
			}
			time.Sleep(1e8)
		}
	}
}
