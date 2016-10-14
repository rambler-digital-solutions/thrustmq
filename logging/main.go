package logging

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"log"
	"net"
	"os"
)

func Init() {
	logfile, err := os.OpenFile(config.Base.Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	common.FaceIt(err)

	log.SetOutput(logfile)
	log.Println("ThrusMQ started")
}

func NewProducer(address net.Addr) {
	log.Printf("new producer %s %s", address.Network(), address.String())
}

func LostProducer(address net.Addr) {
	log.Printf("lost producer %s %s", address.Network(), address.String())
}

func NewConsumer(connectionStruct common.ConnectionStruct, length int) {
	address := connectionStruct.Connection.RemoteAddr()
	log.Printf("new consumer %s %s (%d connections)", address.Network(), address.String(), length)
	log.Printf("id: %d clientId: %d bucketId: %d batchSize: %d", connectionStruct.Id, connectionStruct.ClientId, connectionStruct.Bucket, connectionStruct.BatchSize)
}

func LostConsumer(address net.Addr, length int) {
	log.Printf("lost consumer %s %s (%d connections)", address.Network(), address.String(), length)
}
