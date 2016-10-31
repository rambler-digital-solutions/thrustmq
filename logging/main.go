package logging

import (
	"fmt"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"net"
)

func NewProducer(address net.Addr) {
	message := fmt.Sprintf("new producer %s %s", address.Network(), address.String())
	common.OplogChannel <- &common.OplogRecord{Message: message, Subsystem: "intake", Action: "newProducer"}
}

func LostProducer(address net.Addr) {
	message := fmt.Sprintf("lost producer %s %s", address.Network(), address.String())
	common.OplogChannel <- &common.OplogRecord{Message: message, Subsystem: "intake", Action: "lostProducer"}
}

func NewConsumer(connectionStruct *common.ConnectionStruct, length int) {
	address := connectionStruct.Connection.RemoteAddr()
	message := fmt.Sprintf("new consumer #%d %s %s (%d connections)", connectionStruct.ID, address.Network(), address.String(), length)
	common.OplogChannel <- &common.OplogRecord{Message: message, Subsystem: "exhaust", Action: "newConsumer"}
}

func NewConsumerHeader(connectionStruct *common.ConnectionStruct) {
	message := fmt.Sprintf("consumer #%d subscribed to bucket %d with batch size %d", connectionStruct.ID, connectionStruct.Bucket, connectionStruct.BatchSize)
	common.OplogChannel <- &common.OplogRecord{Message: message, Subsystem: "exhaust"}
}

func LostConsumer(address net.Addr, length int) {
	message := fmt.Sprintf("lost consumer %s %s (%d connections)", address.Network(), address.String(), length)
	common.OplogChannel <- &common.OplogRecord{Message: message, Subsystem: "exhaust", Action: "lostConsumer"}
}
