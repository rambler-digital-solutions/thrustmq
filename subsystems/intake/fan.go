package intake

import (
	"bufio"
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"io"
	"log"
	"net"
)

func getBatchSize(reader *bufio.Reader) int {
	batchSizeBuffer := make([]byte, 4)
	_, err := io.ReadFull(reader, batchSizeBuffer)
	if err != nil {
		return 0
	}
	return int(binary.LittleEndian.Uint32(batchSizeBuffer))
}

func suck(connection net.Conn) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	reader := bufio.NewReaderSize(connection, config.Base.NetworkBuffer)

	for {
		batchSize := getBatchSize(reader)
		ackChannel := make(chan int, batchSize)

		for i := 0; i < batchSize; i++ {
			message := &common.IntakeStruct{}
			message.AckChannel = ackChannel
			message.NumberInBatch = i
			if !message.Deserialize(reader) {
				log.Print("Could not deserialize message...")
				return
			}
			CompressorChannel <- message
		}

		response := make([]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			response[<-ackChannel] = 1
			oplog.IntakeThroughput++
		}

		connection.Write(response)
	}
}
