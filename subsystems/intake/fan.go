package intake

import (
	"bufio"
	"net"
	"thrust/common"
	"thrust/logging"
	"thrust/subsystems/oplog"
)

func suck(connection net.Conn) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	ackChannel := make(chan bool, 1)
	reader := bufio.NewReader(connection)

	for {
		message := common.MessageStruct{AckChannel: ackChannel}
		if !message.Deserialize(reader) {
			return
		}

		CompressorChannel <- message
		<-ackChannel
		connection.Write([]byte{'y'})

		oplog.IntakeThroughput++
	}
}
