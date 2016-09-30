package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"time"
)

func combustion() {
	message := <-CombustorChannel
	for {
		if len(ConnectionsMap) > 0 {
			for _, connectionStruct := range ConnectionsMap {
				if passAndFetchMessage(connectionStruct, message) {
					message = <-CombustorChannel
				}
			}
		} else {
			time.Sleep(1e7)
		}
	}
}

func passAndFetchMessage(connectionStruct common.ConnectionStruct, message common.MessageStruct) bool {
	select {
	case connectionStruct.Channel <- message:
		indexRecord := common.IndexRecord{}
		indexRecord.Connection = connectionStruct.Id
		indexRecord.Position = message.Position
		indexRecord.Ack = 1

		TurbineChannel <- indexRecord

		return true
	}
	return false
}
