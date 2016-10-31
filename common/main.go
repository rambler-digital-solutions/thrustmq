package common

import (
	"github.com/rambler-digital-solutions/thrustmq/config"
)

var (
	State                       = loadState()
	OplogChannel                = make(chan OplogRecord, config.Base.OplogChannelLength)
	ConnectionHeaderSize int    = 20
	MessageHeaderSize    int    = 12
	IndexSize            uint64 = 8 * 9
)
