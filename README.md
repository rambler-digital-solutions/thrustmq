# ThrustMQ

![schema](https://gitlab.rambler.ru/rnd/thrust/raw/develop/schema.png)



We live in a world of real time expectations.

AMPQ (Rabbit, Active, Qpid...) - "They try to do too much."
Log aggregagation (Kafka...) - "No acknolegements"
Network libraries (ZeroMQ,..) - "No persitance"

### Features

NO. Absolutely no dependencies. No zookeeper. No broker.
Just plain old go, without extra packages
1000 lines of open source code.

UNLIMITED number of channels.
Durable all the way.
1000 msg / second

### Design

[diagram here]

### Performance
Competitors:
31250 msg/sec - google (rabbitmq)

### Use cases

Actually MQ is a big theme right now.
Microservice architecture emplys MQ at full scale.
IoT suppose MQ as well.
Log aggregagation, credit cards ... you name it...

### Pacenotes
http://bravenewgeek.com/dissecting-message-queues/
View source of NATS

### Ideas
1. Make Ack optional for publisher & subscriber
1.
