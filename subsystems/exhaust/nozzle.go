package exhaust

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"thrust/logging"
	"thrust/subsystems/common"
	"time"
)

func registerConnect(connection net.Conn, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex) {
	mutex.Lock()
	hash[connection] = make(chan *common.MessageStruct, 1)
	mutex.Unlock()
	logging.NewConsumer(connection.RemoteAddr(), hash)
}

func registerDisconnect(connection net.Conn, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex) {
	mutex.Lock()
	delete(hash, connection)
	mutex.Unlock()
	logging.LostConsumer(connection.RemoteAddr(), hash)
}

func serve(connection net.Conn, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex, counter *uint64) {
	defer registerDisconnect(connection, hash, mutex)

	registerConnect(connection, hash, mutex)

	for {
		select {
		case message := <-hash[connection]:
			log.Println("N: success", len((*message).Payload))
			_, err := connection.Write((*message).Payload)
			if err != nil {
				return
			}
			atomic.AddUint64(counter, 1)
		default:
			log.Println("N: failed (heartbeat, sleep)", hash[connection], len(hash[connection]))
			_, err := connection.Write([]byte{'\n'})
			if err != nil {
				return
			}
			time.Sleep(1e8)
		}
	}
}
