from thrustmq.producer import ThrustMQProducer
from thrustmq.consumer import ThrustMQConsumer

producer = ThrustMQProducer("localhost", 1888)
consumer = ThrustMQConsumer("localhost", 2888)

for i in range(10):
    message = "test message %d" % i
    producer.send([message])

for i in range(10):
    result = consumer.recieve()
    result = str(result, encoding='utf-8')
    print(result)
