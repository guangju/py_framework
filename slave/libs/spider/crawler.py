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
import requests
import json
from libs import log

class Crawler(object):
    """
    classdocs
    """

    HEADERS = {'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Mobile Safari/537.36'}
    
    def __init__(self):
        """
        Constructor
        """
        self.stat = None
        return
    
    def setStatistics(self, stat):
        self.stat = stat
    
    def incrStatKey(self, key):
        if self.stat is not None:
            self.stat.incr(key)
    
    
    def worker(self):
        return
    
    def _real_worker(self, urlPack):
        for _ in range(2):
            try:
                #time.sleep(100)
                log.debug("Fetching: {}".format(urlPack))
                resp = requests.get(urlPack.url, headers=self.HEADERS)
                return json.loads(resp.text)
            except Exception as e:
                log.fatal("crawler_default_real_worker_error:{},{}".format(e, urlPack))
        return False
    
    def fetch(self, urlPack):
        result = self._real_worker(urlPack)
        return result
        
            
        
        
        