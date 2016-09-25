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
	id := rand.Int63()
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
		// time.Sleep(1e6)

		select {
		case message := <-connectionStruct.Channel:

			status := common.ProcessingStruct{Connection: connectionStruct.Id, Offset: message.Position, Ack: false}
			TurbineChannel <- status

			binary.LittleEndian.PutUint32(buffer, uint32(len(message.Payload)))
			_, err := connection.Write(buffer)
			if err != nil {
				return
			}
			_, err = connection.Write(message.Payload)
			if err != nil {
				return
			}

			oplog.ExhaustThroughput += 1

			status.Ack = true
			TurbineChannel <- status

			oplogRecord := oplog.Record{Topic: connectionStruct.Topic, Subsystem: 2, Operation: 1, Offset: 0}
			oplog.Channel <- oplogRecord

			// time.Sleep(1e6)
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

			time.Sleep(1e6)
		}
	}
}
