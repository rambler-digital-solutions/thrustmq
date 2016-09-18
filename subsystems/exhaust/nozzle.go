package exhaust

import (
	"bytes"
	"encoding/binary"
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

func registerDisconnect(connection net.Conn, nozzles *common.MessageChannels, nozzle chan common.MessageStruct, mutex *sync.Mutex, flux common.MessageChannel) {
	mutex.Lock()
	fileteredNozzles := (*nozzles)[:0]
	for _, value := range *nozzles {
		if value != nozzle {
			fileteredNozzles = append(fileteredNozzles, value)
		}
	}
	*nozzles = fileteredNozzles
	mutex.Unlock()
	log.Println("N: Disconnect, fluxing", len(nozzle), "messages")
	for {
		if len(nozzle) == 0 {
			break
		}
		flux <- <-nozzle
	}
	logging.LostConsumer(connection.RemoteAddr(), nozzles)
}

func getHeader(message []byte) []byte {
	sizeBuffer := new(bytes.Buffer)
	var size uint32 = uint32(len(message))
	err := binary.Write(sizeBuffer, binary.LittleEndian, size)
	if err != nil {
		panic("binary.Write failed")
	}
	return sizeBuffer.Bytes()
}

func thrust(connection net.Conn, nozzles *common.MessageChannels, mutex *sync.Mutex, counter *uint64, flux common.MessageChannel) {
	channel := registerConnect(connection, nozzles, mutex)
	defer registerDisconnect(connection, nozzles, channel, mutex, flux)

	for {
		select {
		case message := <-channel:
			_, err := connection.Write(getHeader(message.Payload))
			if err != nil {
				return
			}
			_, err = connection.Write(message.Payload)
			if err != nil {
				return
			}
			atomic.AddUint64(counter, 1)
		default:
			// log.Println("N: x- nozzle (heartbeat, sleep)", channel, len(channel))
			data := []byte{'\n'}
			getHeader(data)
			_, err := connection.Write(getHeader(data))
			if err != nil {
				return
			}
			_, err = connection.Write(data)
			if err != nil {
				return
			}
			time.Sleep(1e6)
		}
	}
}
