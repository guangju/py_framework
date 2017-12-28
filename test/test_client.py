import sys
import os
import socket
sys.path.append('../')
from util.protocal import nshead


if __name__ == '__main__':
    HOST='127.0.0.1'
    PORT=8120
    BUFSIZ=1024
    ADDR=(HOST, PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(ADDR)
    pack = nshead.nshead()
    info = pack.generate()
    nshead.nshead_write(sock, info)
