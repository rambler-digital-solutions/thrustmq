package common

type IndexRecord struct {
	Offset     int64
	Length     int
	Topic      int64
	Connection int64
	Ack        byte
}
