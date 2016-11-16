from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value, Lock, Process

import binascii
import os
import signal
import socket
import sys
import time
import pdb
from message import Message


class ThrustMQConsumer:

    def __init__(self, host="localhost", port=2888, bucket_id=0, batch_size=1):
        self.bucket_id = bucket_id
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.settimeout(1)
        consumer_id = binascii.hexlify(os.urandom(8))
        self.sock.sendall(consumer_id)
        self.sock.sendall(bucket_id.to_bytes(8, byteorder='little'))
        self.sock.sendall(batch_size.to_bytes(4, byteorder='little'))

    def receive(self):
        actual_batch_size = int.from_bytes(
            self.sock.recv(4), byteorder='little')

        messages = []
        acks = bytearray()
        for i in range(actual_batch_size):
            data_size = int.from_bytes(self.sock.recv(4), byteorder='little')
            data = self.sock.recv(data_size)
            messages.append(Message(self.bucket_id, data))
            acks.append(1)
        self.sock.send(acks)

        return messages
