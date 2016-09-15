package subscriber

import (
	"bufio"
	"net"
	"os"
	"thrust/config"
)

func dispatch(updateBus <-chan bool, hash map[net.Conn]chan string) {
	// open file
	queue, err := os.OpenFile(config.Config.Filename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(queue)

	// read lines
	for {
		bytes, _, err := reader.ReadLine()
		// got new line
		if len(bytes) != 0 {
			for _, inbox := range hash {
				inbox <- string(bytes)
			}
		}
		// got EOF wait for new data
		if err != nil {
			<-updateBus
		}
	}
}
