package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func interact(connection net.Conn, log *os.File) {
	reader := bufio.NewReader(connection)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			connection.Close()
			return
		}
		if _, err := log.WriteString(message); err != nil {
			panic(err)
		}
		connection.Write([]byte("y"))
	}
}

func timeit(log *os.File) {
	var old_size float32
	var new_size float32
	for {
		time.Sleep(1 * time.Second)
		fi, _ := log.Stat()
		new_size = float32(fi.Size()) / 1024 / 1024
		fmt.Printf("\r %8.2f MB/s", new_size-old_size)
		old_size = new_size
	}
}

func main() {
	fmt.Println("Launching publisher backend...")
	publisherSocket, _ := net.Listen("tcp", ":1888")
	log, err := os.Create("thrust-queue.txt")
	if err != nil {
		panic(err)
	}

	go timeit(log)

	for {
		connection, _ := publisherSocket.Accept()
		go interact(connection, log)
	}
}
