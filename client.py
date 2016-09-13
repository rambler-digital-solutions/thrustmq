import socket
import os
import binascii
import time
import signal
import sys

HOST = 'localhost'
PORT = 1888
TOKEN = binascii.hexlify(os.urandom(8))
BATCH_SIZE = 10
performance = []

def load():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        message = b'Hello from ' + TOKEN + b'\n'
        stamp = -1
        counter = 0
        while True:
            if int(time.time()) != stamp:
                performance.append(counter)
                stamp = int(time.time())
                counter = 0
            for i in range(BATCH_SIZE):
                s.sendall(message)
                data = s.recv(1)
                # print(data == b'y' and 'Accepted' or 'Rejected')
            counter += BATCH_SIZE
        s.close()

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        print(performance)
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
print('Press Ctrl+C')
load()
