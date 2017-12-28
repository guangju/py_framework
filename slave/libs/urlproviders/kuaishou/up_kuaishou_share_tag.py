# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月5日
#
################################################################################
"""
Created on 2017年12月11日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou
import json


class KuaiShouShareTagProvider(urlprovider.UrlProvider):
    """
    classdocs
    """

    url = "http://m.kuaishou.com/tag/rest/w/tag/feed/text"

    def __init__(self, iterateTime=0):  # @UnusedVariable
        urlprovider.UrlProvider.__init__(self)
        return

    def onReceiveMsg(self, msg):  # @UnusedVariable
        """
        :param msg:
        """
        if msg.msgType == const_kuaishou.DATA_TYPE_TAG_NAME:
            urlPack = urlprovider.UrlPack(priority=20, url=self.url)
            if msg.getExtra('pcursor') == None:
                form = json.dumps({"tag":msg.msgData, "count":20, "pcursor":""})
            else:
                form = json.dumps({"tag":msg.msgData, "count":20, "pcursor":msg.getExtra('pcursor')})

            urlPack.setForm(form)
            urlPack.fillMsg(msg, self.pipe)
            urlPack.addKey('tag', msg.msgData)
            #urlPack.addKey('topic_id', msg.getExtra('topic_id'))
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        urlPack.priority = self.getPipePriority()
        log.debug("KuaiShouShareTagProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False
