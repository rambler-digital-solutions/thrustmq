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
		ackChannel := make(chan *common.IntakeStruct, batchSize)
		messages := make([]*common.IntakeStruct, batchSize)
		for i := 0; i < batchSize; i++ {
			messages[i] = &common.IntakeStruct{}
			messages[i].AckChannel = ackChannel
			messages[i].NumberInBatch = i
			if !messages[i].Deserialize(reader) {
				log.Print("Could not deserialize message...")
				return
			}
			CompressorChannel <- messages[i]
		}

		response := make([]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			message := <-ackChannel
			response[message.NumberInBatch] = message.Status
			oplog.IntakeThroughput++
		}

		connection.Write(response)
	}
}
