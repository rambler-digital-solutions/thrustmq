from multiprocessing import Process

import binascii
import os
import signal
import socket
import sys
import time
from time import gmtime, strftime
import datetime
import random

HOST = 'localhost'
PORT = 1888
TOKEN = binascii.hexlify(os.urandom(8)).decode('utf-8')
POOL_SIZE = 10
HAMMER = 'HAMMER' in os.environ


def send_message(s):
    topic_id = random.choice([1, 2, 3, 3])
    message = 'Привет от воркера %s %d topic_id: %d' % (
        TOKEN,
        int(time.time()),
        topic_id
    )

    if not HAMMER:
        print(message)

    message_bytes = message.encode('utf-8')

    # topic header
    s.sendall(topic_id.to_bytes(8, byteorder='little'))
    # size header
    s.sendall(len(message_bytes).to_bytes(
        4, byteorder='little'))
    # message itself
    s.sendall(message_bytes)

    if not HAMMER:
        sys.stdout.write("  waiting for ack... ")


def load():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                while True:
                    batch_size = 50

                    s.sendall(batch_size.to_bytes(
                        4, byteorder='little'))

                    for i in range(batch_size):
                        send_message(s)

                    result = s.recv(1)

                    if not HAMMER:
                        time.sleep(0.1)
                        if result == b'y':
                            print('ACK')
                        else:
                            print('NO ACK (!)')
        except BrokenPipeError:
            print("BrokenPipeError")
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
