from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value, Lock, Process

import binascii
import os
import signal
import socket
import sys
import time
import pdb

TOKEN = binascii.hexlify(os.urandom(8))

class ThrustMQConsumer:
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.settimeout(1)

    def recieve(self):
        data = self.sock.recv(4)
        size = int.from_bytes(data, byteorder='little')
        message = self.sock.recv(size)
        my_bytes = bytearray()
        my_bytes.append(55)
        self.sock.send(my_bytes)
        return message
