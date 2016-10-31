package oplog

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"time"
)

type channels struct {
	CompressorChannelLength       int
	Stage2CompressorChannelLength int
	CombustorChannelLength        int
	AfterburnerChannelLength      int
	TurbineChannelLength          int
}

type maps struct {
	RecordsMapLength     int
	ConnectionsMapLength int
	BucketsMapLength     int
	ChunksMapLength      int
}

func (object *channels) Update() {
	object.CompressorChannelLength = len(intake.CompressorChannel)
	object.Stage2CompressorChannelLength = len(intake.Stage2CompressorChannel)
	object.CombustorChannelLength = len(exhaust.CombustorChannel)
	object.AfterburnerChannelLength = len(exhaust.AfterburnerChannel)
	object.TurbineChannelLength = len(exhaust.TurbineChannel)
}

func (object *maps) Update() {
	object.RecordsMapLength = len(exhaust.RecordsMap)
	object.ConnectionsMapLength = len(exhaust.ConnectionsMap)
	object.BucketsMapLength = len(exhaust.BucketsMap)
	object.ChunksMapLength = len(exhaust.ChunksMap)
}

type dashboard struct {
	StartedAt         time.Time
	IntakeTotal       int
	ExhaustTotal      int
	IntakeThroughput  int
	ExhaustThroughput int
	Requeued          int
	State             *common.StateStruct
	Channels          *channels
	Maps              *maps
	Config            *config.Struct
}
