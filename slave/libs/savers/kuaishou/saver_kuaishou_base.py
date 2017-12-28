# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年7月1日 上午1:47:11
#
################################################################################
from libs import saver
from libs import log
"""
Created on 2017年12月1日

@author: yangyh
"""

class KuaishouSaverBase(saver.Saver):
    """
    classdocs
    """
    
    def needRetry(self, pipeName, root):
        try:
            if "error_msg" in root:
                log.fatal("kuaishou_fatal_error, pipe={}, error_msg={}".format(pipeName, root['error_msg']))
        except:
            pass
        return False
        