package publisher

import (
	"bufio"
	"net"
)

func serve(connection net.Conn, dumperChannel chan<- string) {
	reader := bufio.NewReader(connection)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			connection.Close()
			return
		}
		dumperChannel <- message
		connection.Write([]byte("y"))
	}
}
