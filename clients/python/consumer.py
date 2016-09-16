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


def timestamp():
    return int(time.time())


def readlines(sock, recv_buffer=4096, delim=b'\n'):
    buffer = bytes()
    data = True
    while data:
        data = sock.recv(recv_buffer)
        print(data)
        # buffer += data
        # try:
        #     while True:
        #         idx = buffer.index(delim)
        #         line = buffer[:idx + 1]
        #         buffer = bytes(buffer[idx:])
        #         pdb.set_trace()
        #
        # except ValueError:
        #     pass
    # for line in str(buffer, encoding='utf-8').split('\n'):
    #     yield line


def load():
    # while True:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))

            for line in readlines(s):
                print(line)
                # pass
    except IOError:
        print('Failed to connect...' + str(timestamp()))
        time.sleep(1)


if __name__ == "__main__":
    load()
