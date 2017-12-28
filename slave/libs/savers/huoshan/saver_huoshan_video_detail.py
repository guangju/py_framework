# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月7日
#
################################################################################
"""
Created on 2017年12月7日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

#from libs.spider.pipe import Message
from libs import log
from libs import saver
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_huoshan, const

class HuoshanVideoDetailSaver(saver.Saver):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.data", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        authorId = data['author']['id']
        del data["author"]
        obj = dbtools.MongoObject()
        obj.setMeta(const.DATA_TYPE_VIDEO, const_huoshan.DATA_PROVIDER, data["id"])
        obj.setData(data)
        obj.setUserId(authorId)
        obj.save()
        log.debug("HuoshanVideoDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
        self.addStatObject(obj.getLastObjectId(), const.DATA_TYPE_VIDEO)

        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.data.author.id"), parse("$.data.id")]
        return self.checkTemplate(video, exprs, urlPack)
