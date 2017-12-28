# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年11月28日
#
################################################################################
"""
Created on 2017年11月28日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs.db import mongo
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou


class KuaiShouMainFeedProvider(urlprovider.UrlProvider):
    """
    classdocs
    """
    #url = "http://api.gifshow.com/rest/n/feed/hot?mod=vivo(vivo%20Y67A)&lon=0&country_code=CN&did=ANDROID_1af1b6170a46b73d&app=0&net=WIFI&oc=ANZHI&ud=0&c=ANZHI&sys=ANDROID_6.0&appver=4.51.0.2360&language=zh-cn&lat=0&ver=4.51&token=&pv=false&client_key=3c2cd3f3&count=20&page=1&type=7&os=android&sig=fe82ef640331de61edca54716621dd56"
    #form = "id={id}&token=&pv=false&client_key=3c2cd3f3&count=20&page=1&type=7&os=android&sig=fe82ef640331de61edca54716621dd56"
    url = "http://api.gifshow.com/rest/n/feed/hot?mod=vivo(vivo%20Y67A)&lon=0&country_code=CN&did=ANDROID_1af1b6170a46b73d&app=0&net=WIFI&oc=ANZHI&ud=0&c=ANZHI&sys=ANDROID_6.0&appver=4.51.0.2360&language=zh-cn&lat=0&ver=4.51&id=1&token=&pv=false&client_key=3c2cd3f3&count=20&page=1&type=7&os=android&sig=fe82ef640331de61edca54716621dd56"
    form = "token=&pv=false&client_key=3c2cd3f3&count=20&page=1&type=7&os=android&sig=fe82ef640331de61edca54716621dd56"
    
    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()

        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("KuaiShouMainFeedProvider receive {}".format(msg))
        if msg.msgType == const_kuaishou.DATA_TYPE_MAINFEED:
            urlPack = urlprovider.UrlPack(priority=0, url=self.url)
            urlPack.setForm(self.form)
            urlPack.fillMsg(msg, self.pipe)
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        info = signature.sign(urlPack.url, signature.gifshow, urlPack.form)
        urlPack.url = info['url']
        urlPack.form = info['form']
        urlPack.priority = self.getPipePriority()
        log.debug("KuaiShouMainFeedProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False
