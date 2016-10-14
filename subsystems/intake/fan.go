package intake

import (
	"bufio"
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"io"
	"net"
)

func getBatchSize(reader *bufio.Reader) int {
	batchSizeBuffer := make([]byte, 4)
	_, err := io.ReadFull(reader, batchSizeBuffer)
	if err != nil {
		return -1
	}
	return int(binary.LittleEndian.Uint32(batchSizeBuffer))
}

func suck(connection net.Conn) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	reader := bufio.NewReaderSize(connection, config.Base.NetworkBuffer)

	for {
		batchSize := getBatchSize(reader)
		if batchSize < 0 {
			return
		}

		ackChannel := make(chan bool, batchSize)
		message := common.MessageStruct{AckChannel: ackChannel}

		for i := 0; i < batchSize; i++ {
			if !message.Deserialize(reader) {
				return
			}
			CompressorChannel <- message
			oplog.IntakeThroughput++
		}

		for i := 0; i < batchSize; i++ {
			<-ackChannel
		}

		connection.Write([]byte{'y'})
	}
}
