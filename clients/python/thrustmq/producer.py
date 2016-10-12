from multiprocessing import Process

import binascii
import os
import signal
import socket
import sys
import time
from time import gmtime, strftime
import datetime
import random

TOKEN = binascii.hexlify(os.urandom(8)).decode('utf-8')
sock = None

class ThrustMQProducer:
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send_message(self, message):
        topic_id = random.choice([1, 2, 3, 3])
        message_bytes = message.encode('utf-8')
        self.sock.sendall(topic_id.to_bytes(8, byteorder='little'))
        self.sock.sendall(len(message_bytes).to_bytes(
            4, byteorder='little'))
        self.sock.sendall(message_bytes)

    def send(self, messages):
        if not isinstance(messages, list):
            messages = [messages]

        batch_size = len(messages)

        self.sock.sendall(batch_size.to_bytes(4, byteorder='little'))

        for message in messages:
            self.send_message(message)

        result = self.sock.recv(1)
