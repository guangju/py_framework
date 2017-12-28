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
"""
Created on 2017年12月1日

@author: yangyh
"""

class DouyinSaverBase(saver.Saver):
    """
    classdocs
    """
    
    def needRetry(self, pipeName, root):  # @UnusedVariable
        return False
        if type(root) != dict:
            return True
        if root.get('status_code') in [0]:
            #0:数据正常
            return False
        return True
        