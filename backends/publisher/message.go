package publisher

type messageStruct struct {
	AckChannel chan bool
	Payload    []byte
}
