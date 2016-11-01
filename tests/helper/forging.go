package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"math/rand"
)

func ForgeProducerMessages(number int) []*producer.Message {
	messages := make([]*producer.Message, number)
	for i := 0; i < number; i++ {
		payload := make([]byte, rand.Intn(1024))
		messages[i] = &producer.Message{}
		messages[i].Length = len(payload)
		messages[i].Payload = payload
	}
	return messages
}
