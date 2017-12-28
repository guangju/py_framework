from __future__ import division, print_function, unicode_literals
from util.protocal import nshead
from util.log import log
import basehandler
import os.path
import json
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        datas = {
            "data":[
                {"name":"allpe","num":100},
                {"name":"peach","num":123},
                {"name":"Pear","num":234},
                {"name":"avocado","num":20},
                {"name":"cantaloupe","num":1},
                {"name":"Banana","num":77},
                {"name":"Grape","num":43},
                {"name":"apricot","num":0},
                {"name":"xuke","num":900}
            ]
        }
        con = json.dumps(datas)
        self.render("index.html",content=con)

class SendMessageHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        '''
        log.notice("in SpiderControlHandler handler")
        sampleId = self.getParamAsString('s')
        if sampleId:
            samples = util.load_file_asdict("./data/spider_add.samples", 0, ":")
            params = util.qs_to_dict(samples[sampleId][0][1])
            pipeName = params["pipe"]
            msgType = params["msg_type"]
            msgData = params["msg_data"]
            priority = params.get("priority", 0)

        else:
            pipeName = self.checkParamAsString("pipe")
            msgType = self.checkParamAsString("msg_type")
            msgData = self.checkParamAsString("msg_data")
            priority = self.getParamAsInt("priority", 0)

        pipe = self.statistics.getPipeByName(pipeName)
        cmd = self.getParamAsString("cmd")
        if cmd == "save":
            self.response_data = pipe.save()
            return
        if cmd == "load":
            self.response_data = pipe.load()
            return

        if pipe is None:
            self.response_data = {
                "added": 0,
                "errmsg": "pipe {} not exist".format(pipeName),
                "msg_type": msgType,
                "msg_data": msgData
            }
            return

        pipeLine = self.getParamAsInt('pipeline', 0)
        msg = Message(msgType, msgData)
        msg.setExtra('priority', priority)
        msg.setExtra('pipeLine', pipeLine)
        #print(msg)
        qsize = pipe.addMessageObject(msg)
        self.response_data = {
            "added": qsize,
            "msg_type": msgType,
            "msg_data": msgData
            }
        '''
        print("aaaaa")
        '''
        datas = {
            "data":[
                {"name":"allpe","num":100},
                {"name":"peach","num":123},
                {"name":"Pear","num":234},
                {"name":"avocado","num":20},
                {"name":"cantaloupe","num":1},
                {"name":"Banana","num":77},
                {"name":"Grape","num":43},
                {"name":"apricot","num":0}
            ]
        }
        content = json.dumps(datas)
        #self.render("index.html", data=content)
        '''
        datas = {
            "data":[
                {"name":"allpe","num":100},
                {"name":"peach","num":123},
                {"name":"Pear","num":234},
                {"name":"avocado","num":20},
                {"name":"cantaloupe","num":1},
                {"name":"Banana","num":77},
                {"name":"Grape","num":43},
                {"name":"apricot","num":0}
            ]
        }
        content = json.dumps(datas)
        self.render("index.html")
        '''
        self.response_data = {
            "added": 'aaa',
            "msg_type": 'bbb',
            "msg_data": 'ccc'
            }
        '''

        return

    def initialize(self, **kwarg):
        """
            init
        """

        pass
