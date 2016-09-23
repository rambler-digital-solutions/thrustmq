package exhaust

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"thrust/common"
	"thrust/config"
	"time"
)

func combustion() {
	indexFile, err := os.OpenFile(config.Config.Index, os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	_, err = indexFile.Seek(state.HeadPosition, os.SEEK_SET)
	common.FaceIt(err)

	dataFile, err := os.OpenFile(config.Config.Data, os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)

	for {
		burnNextMessage(indexFile, dataFile)
	}
}

func burnNextMessage(indexFile *os.File, dataFile *os.File) {
	indexRecord := common.IndexRecord{}
	dec := gob.NewDecoder(indexFile)
	err := dec.Decode(&indexRecord)
	if err != nil {
		time.Sleep(1e9)
		return
	}

	buffer := make([]byte, indexRecord.Length)
	_, err = dataFile.Seek(indexRecord.Offset, os.SEEK_SET)
	common.FaceIt(err)
	_, err = io.ReadFull(dataFile, buffer)
	common.FaceIt(err)

	message := common.MessageStruct{Topic: indexRecord.Topic, Payload: buffer}

	for _, connectionStruct := range ConnectionsMap {
		fmt.Println(len(connectionStruct.Channel))
		select {
		case connectionStruct.Channel <- message:
			fmt.Println("delivered message at", indexRecord.Offset)
		default:
		}
	}
}

//
// func readAndPropagate(shaft <-chan bool, reader *bufio.Reader, nozzles *common.MessageChannels, mutex *sync.Mutex, flux common.MessageChannel) {
// 	for {
// 		var bytes []byte
//
// 		select {
// 		case message := <-flux: // grab messages from dead nozzles first
// 			bytes = message.Payload
// 		default:
// 			bytesFromFile, err := reader.ReadSlice('\n')
// 			if err == io.EOF {
// 				// log.Println("T: awaiting shaft")
// 				<-shaft // wait for new messages
// 				// log.Println("T: done awaiting shaft")
// 			} else {
// 				bytes = bytesFromFile
// 			}
// 		}
//
// 		if len(bytes) != 0 {
// 			propogate(bytes, nozzles, mutex, flux)
// 		}
// 	}
// }
//
// func propogate(data []byte, nozzles *common.MessageChannels, mutex *sync.Mutex, flux common.MessageChannel) {
// 	for {
// 		// time.Sleep(1e9)
//
// 		var nozzle chan common.MessageStruct
// 		message := common.MessageStruct{AckChannel: nil, Payload: data}
//
// 		mutex.Lock()
// 		N := len(*nozzles)
// 		if N != 0 {
// 			nozzle = (*nozzles)[rand.Intn(N)]
// 		}
// 		mutex.Unlock()
//
// 		if N == 0 {
// 			// log.Println("T: no clients (sleeping)")
// 			time.Sleep(1e8) // wait for new consumers
// 		} else {
// 			select {
// 			case nozzle <- message:
// 				// log.Println("T:    nozzle <-")
// 				return
// 			default:
// 				// log.Println("T:    nozzle x-", nozzle, len(nozzle), len(*nozzles))
// 				// channel is full, or connection was closed
// 			}
// 		}
// 	}
// }
