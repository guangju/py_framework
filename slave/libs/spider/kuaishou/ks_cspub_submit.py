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
#import urllib2
#import json
import time
import cPickle
import base64
import traceback
from libs import log
from libs.spider import cspubutil, crawler
try:
    from libs.common import mcpack
except:
    traceback.print_exc()

class CrawlerKuaiShou(crawler.Crawler):
    """
    classdocs
    """
    
    def __init__(self):
        """
        Constructor
        """
        return
    
    def _real_worker(self, urlPack):
        for _ in range(2):
            try:
                json_data = {}
                json_data['target_url'] = urlPack.url
                json_data['method'] = 'POST'
                json_data['request_header'] = "Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: kwai-android"
                json_data['post_data'] = mcpack.RAW(urlPack.form)
                bypass = urlPack.getExtra()
                bypass['submitTime'] = time.time()
                bypass['urlPack'] = base64.b64encode(cPickle.dumps(urlPack, protocol=-1))
                cspubutil.patch(json_data, bypass, urlPack=urlPack)
                failedList = cspubutil.send2cspub([json_data])
                #log.debug("send2cspub_data:{}".format(json_data))
                if len(failedList) > 0:
                    log.fatal("send2cspub_error:{}".format(failedList))
                self.incrStatKey('sub2cspub')
                log.debug("submit2cspub: {}, bypass: {}".format(urlPack, bypass))
                return True
            except Exception as e:
                traceback.print_exc()
                log.fatal("crawlerkuaishou_cspubmodel_real_worker_error:{},{}".format(e, urlPack))
        return False
    
        
   
        
        
        