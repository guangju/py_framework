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
Created on 2017年12月5日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.spider.pipe import Message
from libs.savers.kuaishou import saver_kuaishou_base
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_kuaishou, const
from libs import util
import time

class KuaiShouSearchTagSaver(saver_kuaishou_base.KuaishouSaverBase):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        log.debug("got tag len={}".format(len(data['tags'])))
        for i, info in enumerate(data["tags"]):
            tag_name = info["tag"].strip()
            md5_key = util.md5(tag_name)
            obj = dbtools.MongoObject()
            obj.setMeta(const.DATA_TYPE_TOPIC, const_kuaishou.DATA_PROVIDER, md5_key, version=const_kuaishou.DATA_VERSION)
            obj.setData(info)
            obj.save()
            log.debug("KuaiShouSearchTagSaver Inserting obj {}, tag={}".format(obj.getLastObjectId(), tag_name))
            self.addStatObject(obj.getLastObjectId(), const.DATA_TYPE_TOPIC)
            msg = Message(const_kuaishou.DATA_TYPE_TAG_NAME, info["tag"])
            msg.setExtra("topic_id", md5_key)
            self.publish(msg)
            if i == len(data['tags']) - 1:
                continue
            time.sleep(40)
        return

    def preCheck(self, root, urlPack):
        exprs = [parse("$.tags")]
        return self.checkTemplate(root, exprs, urlPack)
