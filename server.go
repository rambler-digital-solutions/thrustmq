package main

import (
	"thrust/backends/publisher"
	"thrust/backends/subscriber"
)

func main() {
	publisher.Server()
	subscriber.Server()
}
