from producer import ThrustMQProducer
from consumer import ThrustMQConsumer
from message import Message

NUMBER_OF_MESSAGES = 10
BATCH_SIZE = 5
BUCKET_ID = 1

producer = ThrustMQProducer()
consumer = ThrustMQConsumer(bucket_id=BUCKET_ID, batch_size=BATCH_SIZE)

for i in range(int(NUMBER_OF_MESSAGES / BATCH_SIZE)):
    payload = ("test message %d" % i).encode('utf-8')
    messages = [Message(BUCKET_ID, payload) for i in range(BATCH_SIZE)]
    producer.send(messages)

for i in range(int(NUMBER_OF_MESSAGES / BATCH_SIZE)):
    result = consumer.receive()
    print(result)
