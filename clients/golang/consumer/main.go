package consumer

import (
	"fmt"
	"net"
	"os"
)

var connection net.Conn

func Connect() {
	conn, err := net.Dial("tcp", "127.0.0.1:2888")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	connection = conn
}

func Send(data []byte) {
	connection.Write(data)
}

func Recieve(buffer []byte) {
	connection.Read(buffer)
}
