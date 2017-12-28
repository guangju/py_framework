# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年7月1日 上午1:47:11
#
################################################################################
"""
version
"""
from __future__ import division, print_function, unicode_literals
import basehandler
from libs import log, util
from libs.spider.pipe import Message

class SpiderControlHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
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
        
    def initialize(self, **kwarg):
        """
            init
        """
        self.statistics = kwarg['statistics']
        pass
