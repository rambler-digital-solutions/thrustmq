package exhaust

import ()

func combustion() {
	message := <-CombustorChannel
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
