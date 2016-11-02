package intake

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
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
		common.Log("inlet", "con't deserialize message")
		return nil
	}
	return message
}

// Handler for producer connection
func suck(connection net.Conn) {
	address := connection.RemoteAddr()
	message := fmt.Sprintf("new producer %s %s", address.Network(), address.String())
	common.Log("intake", message)
	defer common.Log("intake", fmt.Sprintf("lost producer %s %s", address.Network(), address.String()))

	reader := bufio.NewReaderSize(connection, config.Base.NetworkBuffer)
	for {
		batchSize := getBatchSize(reader)
		if batchSize == 0 {
			common.Log("inlet", "zero batch size, treating like an exit")
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
		}

		connection.Write(response)
	}
}
