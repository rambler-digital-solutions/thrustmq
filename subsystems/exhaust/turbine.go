package exhaust

import (
	"bufio"
	"net"
	"os"
	"thrust/config"
	"thrust/subsystems/common"
)

func dispatch(shaft <-chan bool, hash map[net.Conn]chan common.MessageStruct) {
	// open file
	queue, err := os.OpenFile(config.Config.Filename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(queue)

	// read lines
	for {
		bytes, err := reader.ReadSlice('\n')
		// got new line
		if len(bytes) != 0 {
			for _, inbox := range hash {
				inbox <- common.MessageStruct{AckChannel: nil, Payload: bytes}
			}
		}
		// got EOF wait for new data
		if err != nil {
			<-shaft
		}
	}
}
