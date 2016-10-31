package common

var (
	State                       = loadState()
	OplogChannel                = make(chan *OplogRecord, 1e6)
	ConnectionHeaderSize int    = 20
	MessageHeaderSize    int    = 12
	IndexSize            uint64 = 8 * 9
)
