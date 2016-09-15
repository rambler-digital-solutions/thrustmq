package subscriber

import (
	"net"
	"sync/atomic"
	"thrust/config"
	"thrust/logging"
)

func serve(connection net.Conn, hash map[net.Conn]chan string, counter *uint64) {
	logging.NewConsumer(connection.RemoteAddr())

	inbox := make(chan string, config.Config.Exhaust.TurbineBlades)
	hash[connection] = inbox
	for {
		logging.WatchCapacity("inbox", len(inbox), config.Config.Exhaust.TurbineBlades)

		msg := <-inbox
		byteArray := []byte(msg + "\n")
		_, err := connection.Write(byteArray)
		if err != nil {
			delete(hash, connection)
			return
		}
		atomic.AddUint64(counter, 1)
	}
}
