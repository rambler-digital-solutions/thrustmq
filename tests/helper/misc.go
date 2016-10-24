package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"os"
	"testing"
)

func DumpRecords(records []*common.Record) {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	indexFile.Seek(0, os.SEEK_SET)
	for i := range records {
		indexFile.Write(records[i].Serialize())
	}
	indexFile.Sync()
}

func ForgeConnection(t *testing.T, connectionId uint64, bucketId uint64) {
	consumer.Disconnect()
	CheckConnections(t, 0)

	connection := &common.ConnectionStruct{}
	connection.Id = connectionId
	connection.Bucket = bucketId
	connection.Channel = make(common.RecordPipe, config.Exhaust.NozzleBuffer)

	exhaust.MapConnection(connection)
}
