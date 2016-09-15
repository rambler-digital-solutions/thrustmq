package publisher

import (
	"bufio"
	"net"
	"thrust/config"
	"thrust/logging"
)

func serve(connection net.Conn, dumperChannel chan<- messageStruct) {
	logging.NewProducer(connection.RemoteAddr())
	defer logging.LostProducer(connection.RemoteAddr())

	ackChannel := make(chan bool)
	reader := bufio.NewReader(connection)

	for {
		payload, err := reader.ReadSlice('\n')
		if err != nil {
			return
		}

		logging.WatchCapacity("dumper", len(dumperChannel), config.Config.Publisher.DumperCapacity)

		dumperChannel <- messageStruct{AckChannel: ackChannel, Payload: payload}

		<-ackChannel // recieve acknowledgement

		connection.Write([]byte{'y'})
	}
}
