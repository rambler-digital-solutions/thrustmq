package exhaust

import (
	"encoding/binary"
	"math/rand"
	"net"
	"thrust/common"
	"thrust/config"
	"thrust/logging"
	"thrust/subsystems/oplog"
	"time"
)

func registerConnect(connection net.Conn) common.ConnectionStruct {
	topic := rand.Int63()
	State.ConnectionId += 1
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
}

func blow(connection net.Conn) {
	connectionStruct := registerConnect(connection)
	defer registerDisconnect(connectionStruct)
	buffer := make([]byte, 4)

	for {
		select {
		case message := <-connectionStruct.Channel:

			status := common.IndexRecord{Connection: uint64(connectionStruct.Id), Offset: uint64(message.Position), Ack: 0, Topic: uint64(message.Topic)}
			TurbineChannel <- status

			bytes := message.Serialize()
			bytesWritten, _ := connection.Write(bytes)
			if bytesWritten != len(bytes) {
				connectionStruct.Channel <- message
				return
			}

			oplog.ExhaustThroughput += 1

			status.Ack = 1
			TurbineChannel <- status
		default:
			data := []byte{'#'}

			binary.LittleEndian.PutUint32(buffer, uint32(len(data)))
			_, err := connection.Write(buffer)
			if err != nil {
				return
			}
			_, err = connection.Write(data)
			if err != nil {
				return
			}

			time.Sleep(1e8)
		}
	}
}
