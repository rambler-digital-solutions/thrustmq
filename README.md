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
- Unlimited number of buckets (channels).
- Durable all the way.
- Can transfer up to 28000 messages per second.
- Only 568 lines of beautiful Go code.

### Design
![schema](https://cdn.rawgit.com/rambler-digital-solutions/thrustmq/develop/docs/ThrustMQ.svg)

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

### Protocol

Producer:
```
->
BatchSize uint32
BucketId uint64, Length uint64, data []byte
BucketId uint64, Length uint64, data []byte
...
BucketId uint64, Length uint64, data []byte
```

```
<-
ack []byte
```

Consumer:
```
<-
ConsumerId uint64
BucketId uint64
BatchSize uint32
```

```
->
ActualBatchSize uint32
Length uint64, data []byte
Length uint64, data []byte
...
Length uint64, data []byte
```

```
<-
ack []byte
```
