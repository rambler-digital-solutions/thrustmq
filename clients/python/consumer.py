from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value, Lock, Process

import binascii
import os
import signal
import socket
import sys
import time
import pdb

HOST = 'localhost'
PORT = 2888
TOKEN = binascii.hexlify(os.urandom(8))
HAMMER = 'HAMMER' in os.environ
POOL_SIZE = 10


def timestamp():
    return int(time.time())


def read_message(sock):
    data = sock.recv(4)
    size = int.from_bytes(data, byteorder='little')
    message = sock.recv(size)
    my_bytes = bytearray()
    my_bytes.append(55)
    sock.send(my_bytes)
    return message


def load():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((HOST, PORT))
                sock.settimeout(1)
                while True:
                    data = read_message(sock)
                    if not HAMMER:
                        if len(data) > 0:
                            sys.stdout.write("{}: ".format(len(data)))
                            print(str(data, encoding='utf-8'))
                            time.sleep(1)
                        else:
                            time.sleep(.01)
        except IOError as err:
            print('Failed to connect...' + str(timestamp()))
            print(err)
            time.sleep(1)


def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    for i in range(POOL_SIZE):
        Process(target=load).start()
