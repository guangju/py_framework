#!/usr/bin/env python
#encoding=utf-8

import sys
import time
import socket

class Sender2Socket():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = (host, port)
        self.BUFSZ = 1024

    
    def send(self, data_list):
        if not data_list:
            return True
        for line in data_list:
            ret = self.send_data(line)
            time.sleep(0.1) #发送太频繁会挂掉
            if not ret:
                return ret
        return True

    def send_data(self, string_data):
        if not string_data:
            return True
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self.server)
        except Exception as err:
            sys.stderr.write('%s\n' %err)
            return False
        
        exit_flag = True
        try:
            sock.send(string_data.encode('utf-8'))
        except Exception as err:
            sys.stderr.write('%s\n' %err)
            exit_flag = False
        
        sock.close()
        return exit_flag

if __name__ == "__main__":
    s = Sender2Socket('127.0.0.1', 8026)
    data2send = []
    for line in sys.stdin:
        data2send.append(line.strip())
    s.send(data2send)
