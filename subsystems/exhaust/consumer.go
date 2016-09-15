package exhaust

import (
	"net"
	"sync/atomic"
	"thrust/config"
	"thrust/logging"
	"thrust/subsystems/common"
)

func serve(connection net.Conn, hash map[net.Conn]chan common.MessageStruct, counter *uint64) {
	logging.NewConsumer(connection.RemoteAddr())
	defer logging.LostConsumer(connection.RemoteAddr())

	inbox := make(chan common.MessageStruct, config.Config.Exhaust.TurbineBlades)
	hash[connection] = inbox
	for {
		logging.WatchCapacity("inbox", len(inbox), config.Config.Exhaust.TurbineBlades)

		message := <-inbox
		_, err := connection.Write(message.Payload)
		if err != nil {
			delete(hash, connection)
			return
		}
		atomic.AddUint64(counter, 1)
	}
}
