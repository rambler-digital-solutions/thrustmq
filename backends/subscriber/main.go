package subscriber

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func interact(connection net.Conn, log *os.File) {
	reader := bufio.NewReader(log)
	for {
		str, _, err := reader.ReadLine()
		if err == nil {
			str = append(str, '\n')
			connection.Write(str)
		} else {
			time.Sleep(1e6)
		}
	}
}

func Server() {
	fmt.Println("Launching subscriber backend...")
	publisherSocket, _ := net.Listen("tcp", ":2888")
	log, err := os.Open("thrust-queue.txt")
	if err != nil {
		panic(err)
	}
	for {
		connection, _ := publisherSocket.Accept()
		fmt.Println("New client")
		go interact(connection, log)
	}
}
