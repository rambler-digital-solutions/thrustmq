from multiprocessing import Process

import binascii
import os
import signal
import socket
import sys
import time
from time import gmtime, strftime
import datetime

HOST = 'localhost'
PORT = 1888
TOKEN = binascii.hexlify(os.urandom(8)).decode('utf-8')
POOL_SIZE = 2


def load():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                while True:
                    message = 'Привет от воркера %s %d\n' % (
                        TOKEN, int(time.time()))
                    message = message.encode('utf-8')
                    s.sendall(message)
                    result = s.recv(1)
                    if not result:
                        print('No ack!')
                    else:
                        if result != b'y':
                            print('Invalid reponse oO')
        except BrokenPipeError:
            pass
        except IOError:
            from time import gmtime, strftime
            print('Failed to connect... {} pid:{}'.format(
                strftime("%H:%M:%S", gmtime()), os.getpid()))
            time.sleep(1)


def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    for i in range(POOL_SIZE):
        Process(target=load).start()
