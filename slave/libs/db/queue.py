# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年7月1日 上午1:47:11
#
################################################################################
from __future__ import print_function
from libs.db import redistool
from libs import conftool

"""
Created on 2017年11月30日

@author: yangyh
"""

class PriorityQueue(object):
    """
    classdocs
    """

    def __init__(self, queueName=None):
        """
        Constructor
        """
        self.q = queueName
        
    def enQueue(self, data, priority=9999):
        return redistool.enQueue(data, priority, self.q)
    
    def deQueue(self, withScore=False):
        return redistool.deQueue(self.q, withScore)
    
    def getQueueSize(self):
        return redistool.redisServ.zcard(self.q)
    
    def getMax(self):
        return redistool.redisServ.zrevrange(self.q, 0, 0, withscores=True)


class JobPriorityQueue(PriorityQueue):    
    
    def __init__(self):
        queueName = conftool.cur_env + ":queue2cspub-v2"
        PriorityQueue.__init__(self, queueName)
        
        
class JobBackupQueue(PriorityQueue):    
    
    def __init__(self):
        queueName = conftool.cur_env + ":dequeued-v2"
        PriorityQueue.__init__(self, queueName)        
