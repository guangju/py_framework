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
import urllib2
import json
#import time
from libs import log
from libs.spider import crawler

class CrawlerKuaiShou(crawler.Crawler):
    """
    classdocs
    """

    #HEADERS = {'User-Agent': 'kwai-ios'}
    
    def __init__(self):
        """
        Constructor
        """
        crawler.Crawler.__init__(self)
        return
    
    def _real_worker(self, urlPack):
        for _ in range(2):
            try:
                log.debug("Fetching: {}".format(urlPack))
                resp = urllib2.urlopen(
                    urllib2.Request(urlPack.url, urlPack.form, {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'User-Agent': 'kwai-android'}), timeout=30
                    )
                data = resp.read()
                data = json.loads(data)
                return data
            except Exception as e:
                log.fatal("crawlerkuaishou_syncmodel_real_worker_error:{},{}".format(e, urlPack))
        return False
    
        
            
        
        
        