package intake

import (
	"bufio"
	"net"
	"thrust/config"
	"thrust/logging"
	"thrust/subsystems/common"
)

func serve(connection net.Conn, turbineChannel chan<- common.MessageStruct) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	ackChannel := make(chan bool, 1)
	reader := bufio.NewReader(connection)

	for {
		payload, err := reader.ReadSlice('\n')
		if err != nil {
			return
		}

		logging.WatchCapacity("dumper", len(turbineChannel), config.Config.Intake.CompressorBlades)

		turbineChannel <- common.MessageStruct{AckChannel: ackChannel, Payload: payload}

		<-ackChannel // recieve acknowledgement

		connection.Write([]byte{'y'})
	}
}
