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

    chars = []
    while True:
        a = sock.recv(1)
        chars.append(a)
        if a == "\n" or a == "":
            return "".join(chars)


def readlines(sock, delim=b'\n'):
    buffer = bytes()
    data = True
    while data:
        data = sock.recv(1)
        buffer += data
        if data == b'\n':
            if len(buffer) > 1:
                yield str(buffer, encoding='utf-8')
            buffer = bytes()


def load():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                s.settimeout(1)
                for line in readlines(s):
                    pass
                    # sys.stdout.write(line)
        except IOError:
            print('Failed to connect...' + str(timestamp()))
            time.sleep(1)


if __name__ == "__main__":
    load()
