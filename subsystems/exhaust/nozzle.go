package exhaust

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"thrust/config"
	"thrust/logging"
	"thrust/subsystems/common"
	"time"
)

func registerConnect(connection net.Conn, nozzles *common.MessageChannels, mutex *sync.Mutex) chan common.MessageStruct {
	channel := make(chan common.MessageStruct, config.Config.Exhaust.TurbineBlades)
	mutex.Lock()
	*nozzles = append(*nozzles, channel)
	mutex.Unlock()
	logging.NewConsumer(connection.RemoteAddr(), nozzles)
	return channel
}

func registerDisconnect(connection net.Conn, nozzles *common.MessageChannels, nozzle chan common.MessageStruct, mutex *sync.Mutex) {
	fileteredNozzles := (*nozzles)[:0]
	mutex.Lock()
	for _, value := range *nozzles {
		if value != nozzle {
			fileteredNozzles = append(fileteredNozzles, value)
		}
	}
	mutex.Unlock()
	logging.LostConsumer(connection.RemoteAddr(), nozzles)
}

func thrust(connection net.Conn, nozzles *common.MessageChannels, mutex *sync.Mutex, counter *uint64) {
	channel := registerConnect(connection, nozzles, mutex)
	defer registerDisconnect(connection, nozzles, channel, mutex)

	for {
		select {
		case message := <-channel:
			log.Println("N: success", len(message.Payload))
			_, err := connection.Write(message.Payload)
			if err != nil {
				return
			}
			atomic.AddUint64(counter, 1)
		default:
			log.Println("N: failed (heartbeat, sleep)", channel, len(channel))
			_, err := connection.Write([]byte{'\n'})
			if err != nil {
				return
			}
			time.Sleep(1e8)
		}
	}
}
