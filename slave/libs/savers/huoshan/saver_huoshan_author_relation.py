# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月12日
#
################################################################################
"""
Created on 2017年12月12日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs import saver
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const, const_huoshan

class HuoShanAuthorRelationSaver(saver.Saver):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.data", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        for info in data:
            user = info["user"]
            obj = dbtools.MongoObject()
            obj.setMeta(const.DATA_TYPE_AUTHOR, const_huoshan.DATA_PROVIDER, user["id"])
            top_fans = None
            if "top_fans" in user:
                top_fans = user["top_fans"]
                del user["top_fans"]

            obj.setData(user)
            if not obj.db.isItemUpdatedRecently(obj.key, 3 * 86400):
                obj.save(const.getTable(const.DATA_TYPE_AUTHOR))
                log.debug("HuoShanAuthorRelationSaver Inserting obj {}".format(obj.getLastObjectId()))
                self.addStatObject(obj.getLastObjectId(), const.DATA_TYPE_AUTHOR)
            else:
                log.debug("huoshan uid {} is already inserted".format(user["id"]))

            if top_fans:
                for topf_u in top_fans:
                    topf_obj = dbtools.MongoObject()
                    topf_obj.setMeta(const.DATA_TYPE_AUTHOR, const_huoshan.DATA_PROVIDER, topf_u["id"])
                    topf_obj.setData(topf_u)
                    if not topf_obj.db.isItemUpdatedRecently(topf_obj.key, 3 * 86400):
                        topf_obj.save(const.getTable(const.DATA_TYPE_AUTHOR))
                        log.debug("HuoShanAuthorRelationSaver Inserting obj {}".format(topf_obj.getLastObjectId()))
                        self.addStatObject(topf_obj.getLastObjectId(), const.DATA_TYPE_AUTHOR)
                    else:
                        log.debug("huoshan uid {} is already inserted".format(topf_u["id"]))
        return

    def preCheck(self, root, urlPack):
        exprs = [parse("$.data[*].user"), parse("$.data[*].user.id")]
        return self.checkTemplate(root, exprs, urlPack)
