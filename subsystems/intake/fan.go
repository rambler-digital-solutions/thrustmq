package intake

import (
	"bufio"
	"net"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
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
