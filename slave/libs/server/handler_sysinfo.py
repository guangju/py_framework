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
from pathlib2 import Path


class SysInfoHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in version handler")
        self.response_data = {"spider": self.statistics.getInsertedInfo(), 
                              "queue": self.statistics.getQueueInfo(),
                              "sysver": Path(conftool.root + "/sconf/version").read_text().strip()
                              }
        
    def initialize(self, **kwarg):
        """
            init
        """
        self.statistics = kwarg['statistics']
        pass
