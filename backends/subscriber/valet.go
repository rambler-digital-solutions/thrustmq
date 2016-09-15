package subscriber

import (
	"net"
	"sync/atomic"
)

func serve(connection net.Conn, inbox <-chan string, counter *uint64) {
	for {
		msg := <-inbox
		byteArray := []byte(msg + "\n")
		connection.Write(byteArray)
		atomic.AddUint64(counter, 1)
	}
}
