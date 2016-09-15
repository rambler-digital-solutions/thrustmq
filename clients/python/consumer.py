from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value, Lock, Process

import binascii
import os
import signal
import socket
import sys
import time

HOST = 'localhost'
PORT = 2888
TOKEN = binascii.hexlify(os.urandom(8))
BATCH_SIZE = 1000
POOL_SIZE = 10


def timestamp():
    return int(time.time())


def readlines(sock, recv_buffer=4096, delim='\n'):
    buffer = ''
    data = True
    while data:
        data = sock.recv(recv_buffer)
        buffer += str(data, encoding='utf-8')

        while buffer.find(delim) != -1:
            line, buffer = buffer.split('\n', 1)
            yield line
    return ''


def load():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))

                for line in readlines(s):
                    pass
                    # print(line)
        except IOError:
            print('Failed to connect...' + str(timestamp()))
            time.sleep(1)


def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C')

    load()
