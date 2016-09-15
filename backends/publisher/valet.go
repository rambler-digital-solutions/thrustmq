package publisher

import (
	"bufio"
	"net"
	"thrust/config"
	"thrust/logging"
)

func serve(connection net.Conn, dumperChannel chan<- string) {
	logging.NewProducer(connection.RemoteAddr())

	reader := bufio.NewReader(connection)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			connection.Close()
			return
		}

		logging.WatchCapacity("dumper", len(dumperChannel), config.Config.Publisher.DumperCapacity)

		dumperChannel <- message
		connection.Write([]byte("y"))
	}
}
