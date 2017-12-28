# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月1日
#
################################################################################
"""
Created on 2017年12月1日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou, const
from libs.db import dbtools
from libs.db import mongo


class KuaiShouVideoDetailProvider(urlprovider.UrlProvider):
    """
    classdocs
    """
    url = "http://api.gifshow.com/rest/n/photo/info?mod=vivo(vivo%20Y67A)&lon=116.27265&country_code=CN&did=ANDROID_1af1b6170a46b73d&app=0&net=WIFI&oc=ANZHI&ud=0&c=ANZHI&sys=ANDROID_6.0&appver=4.51.0.2360&language=zh-cn&lat=40.044012&ver=4.51"
    form = "client_key=3c2cd3f3&photoIds={vid}&os=android"

    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("KuaiShouVideoDetailProvider receive {}".format(msg))
        if msg.msgType == const_kuaishou.DATA_TYPE_VIDEO:
            key = dbtools.gen_object_key(const.DATA_TYPE_VIDEO, 'kuaishou', msg.msgData)
            if self.db.getOne(const.getTable('VIDEO'), key, '_key_') is None:
                urlPack = urlprovider.UrlPack(priority=0, url=self.url)
                urlPack.setForm(self.form.format(vid=msg.msgData))
                urlPack.fillMsg(msg, self.pipe)
                self.add(urlPack)
                return True
            else:
                log.debug("vid:{} has already inserted".format(msg.msgData))
        return False

    def next(self):
        urlPack = self.queueGet()
        info = signature.sign(urlPack.url, signature.gifshow, urlPack.form)
        urlPack.url = info['url']
        urlPack.form = info['form']
        urlPack.priority = self.getPipePriority()
        log.debug("KuaiShouVideoDetailProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False
