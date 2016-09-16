package exhaust

import (
	"bufio"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"thrust/config"
	"thrust/subsystems/common"
	"time"
)

func spin(shaft <-chan bool, nozzles *common.MessageChannels, mutex *sync.Mutex) {
	reader := getFileReader()
	readAndPropagate(shaft, reader, nozzles, mutex)
}

func getFileReader() *bufio.Reader {
	queue, err := os.OpenFile(config.Config.Filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	return bufio.NewReader(queue)
}

func readAndPropagate(shaft <-chan bool, reader *bufio.Reader, nozzles *common.MessageChannels, mutex *sync.Mutex) {
	for {
		bytes, err := reader.ReadSlice('\n')

		if len(bytes) != 0 {
			propogate(bytes, nozzles, mutex)
		}

		if err == io.EOF {
			<-shaft // wait for new messages
		}
	}
}

func propogate(data []byte, nozzles *common.MessageChannels, mutex *sync.Mutex) {
	for {
		var nozzle chan common.MessageStruct
		message := common.MessageStruct{AckChannel: nil, Payload: data}

		mutex.Lock()
		N := len(*nozzles)
		if N != 0 {
			nozzle = (*nozzles)[rand.Intn(N)]
		}
		mutex.Unlock()

		if N == 0 {
			log.Println("T: no clients (sleeping)")
			time.Sleep(1e8) // wait for new consumers
		} else {
			select {
			case nozzle <- message:
				log.Println("T: inbox <-")
				return
			default:
				log.Println("T: inbox x-", nozzle, len(nozzle))
				// channel is full, or connection was closed
			}
		}
	}
}
