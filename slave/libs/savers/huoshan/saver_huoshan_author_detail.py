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
Created on 2017年11月22日

@author: yangyh
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs import saver
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const, const_huoshan

class HuoShanAuthorDetailSaver(saver.Saver):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.data", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        #log.debug("HuoShanAuthorDetailSaver", data)
        if type(data) == dict:
            data = [data]
        for user in data:
            obj = dbtools.MongoObject()
            obj.setMeta(const.DATA_TYPE_AUTHOR, const_huoshan.DATA_PROVIDER, user["id"])
            obj.setData(user)
            obj.save(const.getTable(const.DATA_TYPE_AUTHOR))
            log.debug("HuoShanAuthorDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const.DATA_TYPE_AUTHOR)
        return

    def preCheck(self, root, urlPack):
        exprs = [parse("$.data.stats"), parse("$.data.id")]
        return self.checkTemplate(root, exprs, urlPack)
