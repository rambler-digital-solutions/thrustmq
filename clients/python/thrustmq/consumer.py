import socket
from message import Message


class ThrustMQConsumer:

    def __init__(self, host="localhost", port=2888, bucket_id=0, batch_size=1):
        self.bucket_id = bucket_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.settimeout(3)
        consumer_id = (777).to_bytes(8, byteorder='little')
        self.sock.sendall(consumer_id)
        self.sock.sendall(bucket_id.to_bytes(8, byteorder='little'))
        self.sock.sendall(batch_size.to_bytes(4, byteorder='little'))

    def processMessage(self, data_size, messages, acks):
        data = self.sock.recv(data_size)
        messages.append(Message(self.bucket_id, data))
        acks.append(1)

    def receiveBatch(self):
        actual_batch_size = int.from_bytes(
            self.sock.recv(4), byteorder='little')
        data_size = int.from_bytes(self.sock.recv(4), byteorder='little')
        ack = bytearray()
        ack.append(1)

        while actual_batch_size == 1 and data_size == 0:
            self.sock.send(ack)
            actual_batch_size = int.from_bytes(
                self.sock.recv(4), byteorder='little')
            data_size = int.from_bytes(self.sock.recv(4), byteorder='little')

        messages = []
        acks = bytearray()
        self.processMessage(data_size, messages, acks)

        for i in range(actual_batch_size - 1):
            data_size = int.from_bytes(self.sock.recv(4), byteorder='little')
            self.processMessage(data_size, messages, acks)

        self.sock.send(acks)

        return messages
