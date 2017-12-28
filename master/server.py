import socket
import select
import traceback
import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
import Queue
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.options
from util.protocal import nshead
from util.log import log
import basehandler
import send_message
import os.path

class MasterServer(threading.Thread):

    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = int(port)
        self.host = '127.0.0.1'

    def startServer(self):
        app = tornado.web.Application(
            handlers=[
                    (r"/msg", send_message.SendMessageHandler, dict(initializer=None)),
                    (r"/test", send_message.IndexHandler)
                    ],
            template_path=os.path.join(os.path.dirname(__file__),"template")
            )

        http_server = tornado.httpserver.HTTPServer(app)
        http_server.bind(self.port)
        #MUST BY 1, for threading share data
        process_num = 5
        log.debug("Started Server, port={}".format(self.port))

        http_server.start(process_num if process_num > 0 else 1)
        log.debug("process_num:%s, current_pid:%d" % (process_num, multiprocessing.current_process().pid))
        try:
            tornado.ioloop.IOLoop.instance().start()
        except Exception as e:
            sys.stderr.write("Exception: {}".format(e))

    def run(self):
        self.startServer()
