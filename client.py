from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value, Lock, Process

import binascii
import os
import signal
import socket
import sys
import time

HOST = 'localhost'
PORT = 1888
TOKEN = binascii.hexlify(os.urandom(8))
BATCH_SIZE = 1000
POOL_SIZE = 10


def measure(counter, value):
    stamp = -1
    while True:
        if int(time.time()) != stamp:
            sys.stdout.write(
                "\r{:>4}k msg/sec ".format(counter.value // BATCH_SIZE))
            stamp = int(time.time())
            with lock:
                counter.value = 0


def load(counter, value):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        message = 'Привет от воркера %s \n' % str(TOKEN)
        message = message.encode('utf-8')

        while True:
            for i in range(BATCH_SIZE):
                s.sendall(message)
                if s.recv(1) != b'y':
                    sys.exit(1)
            with lock:
                counter.value += BATCH_SIZE

        s.close()


def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C')

    counter = Value('i', 0)
    lock = Lock()
    for i in range(POOL_SIZE):
        Process(target=load, args=(counter, lock)).start()

    measure(counter, lock)
