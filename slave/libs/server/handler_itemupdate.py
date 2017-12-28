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
from libs import log
from libs.db import mongo, dbtools
from libs.const import const


class ItemUpdateHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in ItemInfoHandler handler")
        key = self.checkParamAsString('key')
        db = mongo.DB()
        table = const.getTable(key)
        resp = db.getOne(table, dbtools.get_object_id_by_key(key))
        adapter = __import__('libs.adapter.adapter_' + str(key.split("-")[1]), fromlist=["libs.adapter"])
        resp = adapter.transformAuthorDetail(resp)
        self.response_data = resp
        
    def initialize(self, **kwarg):
        """
            init
        """
        self.statistics = kwarg['statistics']
        pass
