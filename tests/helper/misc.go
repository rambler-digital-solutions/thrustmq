package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
)

func DumpRecords(records []*common.Record) {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+"0", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	indexFile.Seek(0, os.SEEK_SET)
	dataFile, err := os.OpenFile(config.Base.DataPrefix+"0", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile.Seek(0, os.SEEK_SET)

	common.State.DataWriteOffset = 0
	common.State.UndeliveredOffset = 0
	common.State.WriteOffset = 0

	for i := range records {
		dataFile.Write(records[i].Data)
		records[i].DataSeek = common.State.DataWriteOffset
		common.State.DataWriteOffset += uint64(len(records[i].Data))
		indexFile.Write(records[i].Serialize())
		records[i].Seek = common.IndexSize * uint64(i)
	}
	indexFile.Sync()
	dataFile.Sync()

	common.State.WriteOffset = common.IndexSize * uint64(len(records))
}
