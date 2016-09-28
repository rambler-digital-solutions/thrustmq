# ThrustMQ (WIP)

Plain Golang message queue designed for speed, durability & simplicity.

How ThrustMQ stands against competition:

AMPQ (Rabbit, Active, Qpid, ...) - "They try to do too much".
<br />
Log aggregation (Kafka, ...) - "No acknowledgements".
<br />
Network libraries (ZeroMQ, ...) - "No persistence".

### Features

- No. Absolutely no dependencies. Just plain old go, without extra packages
- Unlimited number of topics (channels).
- Durable all the way.
- Can transfer up to 28000 messages per second.
- Only 568 lines of beautiful Go code.

### Design
![schema](https://raw.githubusercontent.com/rambler-digital-solutions/thrustmq/develop/schema.png)

### Installation
[Install golang](https://golang.org/doc/install).
```
go get github.com/rambler-digital-solutions/thrustmq
```

### Quickstart
```bash
go run thrust.go
python clients/python/producer.py
python clients/python/consumer.py
```
