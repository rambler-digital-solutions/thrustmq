package exhaust

import (
	"math/rand"
	"net"
	"thrust/common"
	"thrust/config"
	"thrust/logging"
	"thrust/subsystems/oplog"
	"time"
)

func registerConnect(connection net.Conn) common.ConnectionStruct {
	topic := uint64(rand.Int63())
	State.ConnectionId++
	id := State.ConnectionId
	channel := make(chan common.MessageStruct, config.Config.Exhaust.TurbineBuffer)

	connectionStruct := common.ConnectionStruct{Connection: connection, Topic: topic, Id: id, Channel: channel}
	connectionStruct.Channel = make(common.MessageChannel, 1000)

	ConnectionsMap[id] = connectionStruct

	logging.NewConsumer(connection.RemoteAddr(), len(ConnectionsMap))
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
	connectionStruct := registerConnect(connection)
	defer registerDisconnect(connectionStruct)
	blankMessage := common.MessageStruct{}
	blankBytes := blankMessage.Serialize()

	for {
		select {
		case message := <-connectionStruct.Channel:
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

			TurbineChannel <- common.IndexRecord{Connection: connectionStruct.Id, Position: message.Position, Ack: 2}
		default:
			bytesWritten, err := connection.Write(blankBytes)
			if err != nil || bytesWritten != len(blankBytes) {
				return
			}
			time.Sleep(1e8)
		}
	}
}
