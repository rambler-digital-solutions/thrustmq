package intake

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"net"
)

func suck(connection net.Conn) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	ackChannel := make(chan bool, 1)
	reader := bufio.NewReader(connection)
	message := common.MessageStruct{AckChannel: ackChannel}

	for {
		if !message.Deserialize(reader) {
			return
		}

		CompressorChannel <- message
		<-ackChannel
		connection.Write([]byte{'y'})

		oplog.IntakeThroughput++
	}
}
