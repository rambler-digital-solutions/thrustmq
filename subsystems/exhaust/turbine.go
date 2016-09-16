package exhaust

import (
	"bufio"
	"io"
	// "log"
	"math/rand"
	"os"
	"sync"
	"thrust/config"
	"thrust/subsystems/common"
	"time"
)

func spinTurbine(shaft <-chan bool, nozzles *common.MessageChannels, mutex *sync.Mutex, flux common.MessageChannel) {
	reader := getFileReader()
	readAndPropagate(shaft, reader, nozzles, mutex, flux)
}

func getFileReader() *bufio.Reader {
	queue, err := os.OpenFile(config.Config.Filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	return bufio.NewReader(queue)
}

func readAndPropagate(shaft <-chan bool, reader *bufio.Reader, nozzles *common.MessageChannels, mutex *sync.Mutex, flux common.MessageChannel) {
	for {
		var bytes []byte

		select {
		case message := <-flux: // grab messages from dead nozzles first
			bytes = message.Payload
		default:
			bytesFromFile, err := reader.ReadSlice('\n')
			if err == io.EOF {
				// log.Println("T: awaiting shaft")
				<-shaft // wait for new messages
				// log.Println("T: done awaiting shaft")
			} else {
				bytes = bytesFromFile
			}
		}

		if len(bytes) != 0 {
			propogate(bytes, nozzles, mutex, flux)
		}
	}
}

func propogate(data []byte, nozzles *common.MessageChannels, mutex *sync.Mutex, flux common.MessageChannel) {
	for {
		// time.Sleep(1e9)

		var nozzle chan common.MessageStruct
		message := common.MessageStruct{AckChannel: nil, Payload: data}

		mutex.Lock()
		N := len(*nozzles)
		if N != 0 {
			nozzle = (*nozzles)[rand.Intn(N)]
		}
		mutex.Unlock()

		if N == 0 {
			// log.Println("T: no clients (sleeping)")
			time.Sleep(1e8) // wait for new consumers
		} else {
			select {
			case nozzle <- message:
				// log.Println("T:    nozzle <-")
				return
			default:
				// log.Println("T:    nozzle x-", nozzle, len(nozzle), len(*nozzles))
				// channel is full, or connection was closed
			}
		}
	}
}
