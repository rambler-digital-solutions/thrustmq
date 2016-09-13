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
TOKEN = binascii.hexlify(os.urandom(8)).decode('utf-8')
BATCH_SIZE = 1000
POOL_SIZE = 1


def timestamp():
    return int(time.time())


def measure(counter, value):
    while True:
        time.sleep(1)
        # sys.stdout.write(
        #     "\r{:>4}k msg/sec ".format(counter.value // BATCH_SIZE))
        with lock:
            counter.value = 0


def load(counter, value):
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))

                while True:
                    for i in range(BATCH_SIZE):
                        message = 'Привет от воркера %s %d\n' % (
                            TOKEN, int(time.time()))
                        message = message.encode('utf-8')
                        s.sendall(message)
                        result = s.recv(1)
                        if not result:
                            raise IOError()
                        if result != b'y':
                            print('Dramatic error!')
                            sys.exit(1)
                        time.sleep(2)
                    with lock:
                        counter.value += BATCH_SIZE
        except IOError:
            print('Failed to connect...' + str(timestamp()))
            time.sleep(1)


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
