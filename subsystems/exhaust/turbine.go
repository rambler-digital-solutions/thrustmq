package exhaust

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"thrust/config"
	"thrust/subsystems/common"
	"time"
)

func spin(shaft <-chan bool, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex) {
	reader := getFileReader()
	readAndPropagate(shaft, reader, hash, mutex)
}

func getFileReader() *bufio.Reader {
	queue, err := os.OpenFile(config.Config.Filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	return bufio.NewReader(queue)
}

func readAndPropagate(shaft <-chan bool, reader *bufio.Reader, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex) {
	log.Println("entered readAndPropagate")
	for {
		bytes, err := reader.ReadSlice('\n')

		if len(bytes) != 0 {
			propogate(bytes, hash, mutex)
		}

		if err == io.EOF {
			<-shaft // wait for new messages
		}
	}
}

func propogate(data []byte, hash map[net.Conn]chan *common.MessageStruct, mutex *sync.Mutex) {
	log.Println("T: spinning!", len(data))
	for {
		if len(hash) == 0 {
			log.Println("T: no clients (sleeping)")
			time.Sleep(1e8) // wait for new consumers
		} else {
			sent := false
			mutex.Lock()
			for _, inbox := range hash {
				message := common.MessageStruct{AckChannel: nil, Payload: data}
				select {
				case inbox <- &message:
					log.Println("T: inbox <-")
					sent = true
				default:
					log.Println("T: inbox x-", inbox, len(inbox))
					// channel is full, or connection was closed
				}
			}
			mutex.Unlock()
			if sent {
				return
			} else {
				log.Println("T: failed (sleeping)")
				time.Sleep(1e8) // wait for new consumers
			}
		}
	}
}
