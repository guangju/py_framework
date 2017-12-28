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
Created on 2017年11月22日

@author: yangyh
"""

from __future__ import division, print_function, unicode_literals
from libs import log
import traceback
from jsonpath_rw import parse

class Saver(object):
    """
    classdocs
    """

    
    def __init__(self, params=None):  # @UnusedVariable
        """
        Constructor
        """
        self.xpath_actions = []
        self.pipe = None
    
    def publish(self, msg):
        self.pipe.publish(msg)
    
    def setPipe(self, pipe):
        self.pipe = pipe
    
    def preCheck(self, data, urlPack):
        raise NotImplementedError("Should have implemented this")
    
    def executeAction(self, resp, urlPack):
        self._execute_action(self.xpath_actions, resp, urlPack)
    
    def start(self, resp, urlPack):
        if self.preCheck(resp, urlPack):
            self.executeAction(resp, urlPack)
            return True
        return False

    def addStatObject(self, objectId, objectType="default"):
        if self.pipe and self.pipe.stat:
            self.pipe.stat.addInserted(objectId, objectType)
        return

    def register_action(self, xpath, action):
        self.xpath_actions.append([parse(xpath) if xpath is not None else None, action])

    def _execute_action(self, path_actions, root, urlPack):
        """
        execute xpath action
        """
        for xa in path_actions:
            pattern = xa[0]
            func = xa[1]
            if pattern is None:
                func(root, None)
            else:
                r = pattern.find(root)
                for match in r:
                    try:
                        func(root, match.value, urlPack)
                    except Exception as e:
                        traceback.print_exc()
                        log.fatal("_execute_action_error:{}, match.value:{}".format(func, match.value), e)
                if len(r) == 0:
                    log.warning("pattern {} match empty!".format(pattern))
                    
    def checkTemplate(self, root, exprs, urlPack):
        for e in exprs:
            if len(e.find(root)) == 0:
                log.fatal("{} precheck_error, url={}, resp={}".format(self.pipe.name, urlPack, root), str(e))
                self.pipe.incTemplateError()
                return False
        return True
    
    def needRetry(self, pipeName, root):  # @UnusedVariable
        return False
