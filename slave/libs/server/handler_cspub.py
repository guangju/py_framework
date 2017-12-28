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
from libs.sender import send2cspub as send

REQUEST_URL = 'http://szwg-mco-tvcloud00.szwg01:8073/job?cmd=get'

DEST_HOST = '10.95.28.43'
DEST_PORT = 8040


class CspubHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in CspubHandler handler")
        resp = send.send2cspub(req_url = REQUEST_URL, dest_host = DEST_HOST, dest_port = DEST_PORT)
        if 'code' in resp and resp['code'] == 0 and 'data' in resp:
            self.response_data = {'csdata': resp['data'], 'csresp': True}
        else:
            self.response_data = {'csdata': resp['data'], 'csresp': False}
        
    def initialize(self, **kwarg):
        """
            init
        """
        self.statistics = kwarg['statistics']
        pass

