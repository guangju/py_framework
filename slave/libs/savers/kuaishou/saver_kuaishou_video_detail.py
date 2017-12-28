# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月1日
#
################################################################################
"""
Created on 2017年12月1日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.savers.kuaishou import saver_kuaishou_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_kuaishou
import time

class KuaiShouVideoDetailSaver(saver_kuaishou_base.KuaishouSaverBase):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.photos", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        if type(data) == dict:
            data = [data]

        for info in data:
            info[self.pipe.name] = int(time.time())
            obj = dbtools.MongoObject()
            obj.setMeta(const_kuaishou.DATA_TYPE_VIDEO, const_kuaishou.DATA_PROVIDER, info["photo_id"], version=const_kuaishou.DATA_VERSION)
            obj.setData(info)
            obj.setUserId(info['user_id'])
            obj.save(const_kuaishou.MONGO_TABLE_VIDEO)
            log.debug("KuaiShouVideoDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_kuaishou.DATA_TYPE_VIDEO)
            #authorId = info['user_id']

        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.photos[*].main_mv_urls")]
        return self.checkTemplate(video, exprs, urlPack)
