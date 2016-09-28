package exhaust

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
)

func combustion() {
	message := <-CombustorChannel
	for {
		for _, connectionStruct := range ConnectionsMap {
			select {
			case connectionStruct.Channel <- message:
				TurbineChannel <- common.IndexRecord{Connection: connectionStruct.Id, Position: message.Position, Ack: 1}
				message = <-CombustorChannel
			default:
				break
			}
		}
	}
}
