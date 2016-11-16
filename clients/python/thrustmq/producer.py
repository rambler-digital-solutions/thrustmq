import socket
from message import Message


class ThrustMQProducer:

    def __init__(self, host="localhost", port=1888):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send_message(self, message):
        self.sock.sendall(message.bucket_id.to_bytes(8, byteorder='little'))
        self.sock.sendall(message.length.to_bytes(4, byteorder='little'))
        self.sock.sendall(message.data)

    def send(self, messages):
        if not isinstance(messages, list):
            messages = [messages]

        batch_size = len(messages)

        self.sock.sendall(batch_size.to_bytes(4, byteorder='little'))

        for message in messages:
            self.send_message(message)

        result = self.sock.recv(message.length)
