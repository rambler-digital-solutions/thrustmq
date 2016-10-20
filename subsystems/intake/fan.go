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
		return 0
	}
	return int(binary.LittleEndian.Uint32(batchSizeBuffer))
}

func getMessage(i int, ackChannel chan *common.IntakeStruct, reader *bufio.Reader) *common.IntakeStruct {
	message := &common.IntakeStruct{}
	message.AckChannel = ackChannel
	message.NumberInBatch = i
	if !message.Deserialize(reader) {
		return nil
	}
	return message
}

func suck(connection net.Conn) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	reader := bufio.NewReaderSize(connection, config.Base.NetworkBuffer)
	for {
		batchSize := getBatchSize(reader)
		if batchSize == 0 {
			return
		}

		ackChannel := make(chan *common.IntakeStruct, batchSize)
		messages := make([]*common.IntakeStruct, batchSize)
		response := make([]byte, batchSize)

		for i := 0; i < batchSize; i++ {
			messages[i] = getMessage(i, ackChannel, reader)
			if messages[i] == nil {
				return
			}
			CompressorChannel <- messages[i]
		}

		for i := 0; i < batchSize; i++ {
			message := <-ackChannel
			response[message.NumberInBatch] = message.Status
			oplog.IntakeThroughput++
		}

		connection.Write(response)
	}
}
