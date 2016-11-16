# ThrustMQ (WIP)

[![Build Status](https://travis-ci.org/rambler-digital-solutions/thrustmq.svg?branch=master)](https://travis-ci.org/rambler-digital-solutions/thrustmq)
[![Code Climate](https://codeclimate.com/github/rambler-digital-solutions/thrustmq/badges/gpa.svg)](https://codeclimate.com/github/rambler-digital-solutions/thrustmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/rambler-digital-solutions/thrustmq)](https://goreportcard.com/report/github.com/rambler-digital-solutions/thrustmq)

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
bucketID uint64, Length uint64, data []byte
bucketID uint64, Length uint64, data []byte
...
bucketID uint64, Length uint64, data []byte
```

```
<-
ack []byte
```

Consumer:
```
<-
ConsumerId uint64
bucketID uint64
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

### JSON dashboard
Navigate to `http://localhost:3888/dash` to get a snapshot of ThrustMQ internals.

### Tests
```
go test ./tests/...
go test -bench=. -benchmem ./benchmarks/...
```
