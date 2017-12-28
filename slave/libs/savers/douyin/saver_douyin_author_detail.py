# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年11月30日
#
################################################################################
"""
Created on 2017年11月30日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.savers.douyin import saver_douyin_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_douyin
from libs.spider.pipe import Message

class DouyinAuthorDetailSaver(saver_douyin_base.DouyinSaverBase):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handleUserDetail)
        return

    def handleUserDetail(self, root, data, urlPack):  # @UnusedVariable
        uid = data["user"]["uid"]
        obj = dbtools.MongoObject()
        obj.setMeta(const_douyin.DATA_TYPE_AUTHOR, const_douyin.DATA_PROVIDER, uid, version=const_douyin.DATA_VERSION)
        obj.setData(data["user"])
        obj.save(const_douyin.MONGO_TABLE_AUTHOR)
        log.debug("DouyinAuthorDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
        self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_AUTHOR)
        self.pipe.publish(Message(const_douyin.DATA_TYPE_AUTHOR, uid))
        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.user.uid")]
        return self.checkTemplate(video, exprs, urlPack)
