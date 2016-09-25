package exhaust

import (
	"thrust/common"
)

func combustion() {
	var message common.MessageStruct
	for {
		for _, connectionStruct := range ConnectionsMap {
			select {
			case connectionStruct.Channel <- message:
				message = <-CombustorChannel
			default:
			}
		}
	}
}
