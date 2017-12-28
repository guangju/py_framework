#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
slave server
"""

import socket
import select
import traceback
import threading
import Queue
from util.log import log
from util.protocal import nshead
from slave.taskall import parallel
from slave import handler



class SlaveServer(threading.Thread):

    def __init__(self, port):
        threading.Thread.__init__(self)
        self.running = True
        self.port = port

    def _run(self, port=8000):
        """

        """

        self.task_pool = parallel.pool(pool_size=5)

        Server = ("0.0.0.0", port)
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(Server)
        server_sock.listen(5)
        server_sock.setblocking(0)

        epoll = select.epoll()  # @UndefinedVariable
        epoll.register(server_sock.fileno(), select.EPOLLIN)  # @UndefinedVariable

        connections = {}
        requests = {}
        log.debug("Starting callback at port {}".format(port))
        while self.running:
            events = epoll.poll()
            #print(events)
            #print("select.EPOLLIN={}".format(select.EPOLLIN))
            #print("select.EPOLLHUP={}".format(select.EPOLLHUP))
            for fileno, event in events:
                if fileno == server_sock.fileno():
                    connection, addr = server_sock.accept()
                    log.debug("accepted socket from {}".format(addr))

                    connection.setblocking(0)
                    epoll.register(connection.fileno(), select.EPOLLIN)  # @UndefinedVariable
                    connections[connection.fileno()] = connection
                    requests[connection.fileno()] = b''
                elif event & select.EPOLLIN:  # @UndefinedVariable
                    recv_data = connections[fileno].recv(104857600)
                    need_close_conn = False
                    #time.sleep(3)
                    if not recv_data:
                        need_close_conn = True
                    else:
                        requests[fileno] += recv_data
                        while len(requests[fileno]) >= nshead.nsead_body_len:
                            receive_nshead = nshead.nshead()
                            try:
                                receive_nshead.load(requests[fileno][:nshead.nsead_body_len])
                                packet_length = nshead.nsead_body_len + receive_nshead.head['body_len']
                                if len(requests[fileno]) >= packet_length:
                                    data = requests[fileno][nshead.nsead_body_len: packet_length]

                                    self.task_pool.add_task(handler.task_handler, data)

                                    requests[fileno] = requests[fileno][packet_length:]
                                else:
                                    break
                            except Exception as e:
                                need_close_conn = True
                                print traceback.format_exc()
                                log.fatal("cspub_receiver_error", e)
                                break

                    if need_close_conn:
                        epoll.unregister(fileno)
                        connections[fileno].close()
                        del connections[fileno]
                        del requests[fileno]
                        print "connection closed"

                elif event & select.EPOLLHUP:  # @UndefinedVariable
                    epoll.unregister(fileno)
                    connections[fileno].close()
                    del connections[fileno]
                    del requests[fileno]

    def stop(self):
        self.running = False

    def run(self):
        self._run(port=int(self.port))
