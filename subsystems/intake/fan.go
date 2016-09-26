package intake

import (
	"bufio"
	"net"
	"thrust/common"
	"thrust/logging"
	"thrust/subsystems/exhaust"
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
			continue
		}
		// send message to compressor
		CompressorChannel <- message
		// receive acknowledgement
		<-ackChannel
		// send message to combustor
		select {
		case exhaust.CombustorChannel <- message:
		default:
		}
		// send ack to producer
		connection.Write([]byte{'y'})

		oplog.IntakeThroughput += 1
	}
}
