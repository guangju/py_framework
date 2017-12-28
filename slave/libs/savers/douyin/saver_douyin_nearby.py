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
Created on 2017年12月13日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.savers.douyin import saver_douyin_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_douyin
from libs.spider.pipe import Message

class DouyinNearBySaver(saver_douyin_base.DouyinSaverBase):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.aweme_list", self.handleUserDetail)
        return

    def handleUserDetail(self, root, data, urlPack):  # @UnusedVariable
        for info in data:
            try:
                user = info["author"]
                uid = user["uid"]
                obj = dbtools.MongoObject()
                obj.setMeta(const_douyin.DATA_TYPE_AUTHOR,
                            const_douyin.DATA_PROVIDER,
                            uid,
                            version=const_douyin.DATA_VERSION)
                obj.setData(user)
                if not obj.db.isItemUpdatedRecently(obj.key):
                    obj.save(const_douyin.MONGO_TABLE_AUTHOR)
                    log.debug("DouyinAuthorDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
                    self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_AUTHOR)
                else:
                    log.debug("uid:{} is already inserted".format(uid))
            except Exception as e:
                log.fatal("{}".format(e))
                raise e
        return


    def preCheck(self, video, urlPack):
        exprs = [parse("$.aweme_list")]
        #exprs = [parse("$.aweme_list")]
        return self.checkTemplate(video, exprs, urlPack)
