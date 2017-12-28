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
from libs import log, conftool
from libs.db import mongo
from libs.adapter import adaptertool


CALLBACK_HOST = conftool.cf.get('sender_callback', 'host')
CALLBACK_PORT = conftool.cf.get('sender_callback', 'port')

class CmsHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in CmsHandler handler")
        ksid = self.checkParamAsString('ksid')
        db = mongo.DB()
        authorInfo = db.find('m_author', {'profile.kwaiId': ksid}, 1)
        if authorInfo is None or len(authorInfo) == 0:
            self.response_data = {"notice": "未收录作者信息"}
            return
        authorInfo = authorInfo[0]
        uid = adaptertool.getUid(authorInfo)
        count = db.getCollection('m_video').find({'_dataSource_': 'kuaishou', '_user3rdId_': str(uid)}).count()   
        resp = {"_video_count_": count, '_authorInfo_': authorInfo}
        self.response_data = resp
        
    def initialize(self, **kwarg):
        """
            init
        """
        pass
