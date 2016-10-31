package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"log"
	"os"
	"testing"
)

func DumpRecords(records []*common.Record) {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+"0", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	indexFile.Seek(0, os.SEEK_SET)
	dataFile, err := os.OpenFile(config.Base.DataPrefix+"0", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile.Seek(0, os.SEEK_SET)

	common.State.NextDataWriteOffset = 0

	for i := range records {
		log.Print("dumping ", records[i].Data)
		dataFile.Write(records[i].Data)
		records[i].DataSeek = common.State.NextDataWriteOffset
		common.State.NextDataWriteOffset += uint64(len(records[i].Data))
		indexFile.Write(records[i].Serialize())
	}
	indexFile.Sync()
	dataFile.Sync()
	common.State.UndeliveredOffset = 0
	common.State.NextWriteOffset = common.IndexSize * uint64(len(records))
}

func ForgeConnection(t *testing.T, connectionID uint64, bucketID uint64) {
	consumer.Disconnect()
	CheckConnections(t, 0)
	connection := &common.ConnectionStruct{}
	connection.ID = connectionID
	connection.Bucket = bucketID
	connection.Channel = make(common.RecordPipe, config.Exhaust.NozzleBuffer)
	exhaust.MapConnection(connection)
	exhaust.RegisterBucketSink(connection)
}
