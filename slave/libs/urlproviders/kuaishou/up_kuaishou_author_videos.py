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
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou


class KuaiShouAuthorVideosProvider(urlprovider.UrlProvider): 
    """
    classdocs
    """
    url = "http://api.gifshow.com/rest/n/feed/profile2?app=0&lon=116.274752&c=ANZHI&sys=ANDROID_4.4.2&mod=HUAWEI(H60-L02)&did=ANDROID_3353b26d0c458dae&ver=4.56&net=WIFI&country_code=CN&appver=4.56.4.4454&oc=UNKNOWN&ud=0&language=zh-cn&lat=40.043655"
    form_next = "os=android&client_key=3c2cd3f3&count=30&token=&pcursor={pcursor}&privacy=public&user_id={user_id}&referer=ks%3A%2F%2Fprofile%2F{user_id}%2F-1%2F-1&lang=zh"
    form_first = "os=android&client_key=3c2cd3f3&count=30&token=&privacy=public&user_id={user_id}&referer=ks%3A%2F%2Fprofile%2F{user_id}%2F-1%2F-1&lang=zh"
    
    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        return
    
    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("KuaiShouAuthorVideosProvider receive {}".format(msg))
        if msg.msgType == const_kuaishou.DATA_TYPE_AUTHOR:
            urlPack = urlprovider.UrlPack(priority=0, url=self.url)
            if msg.getExtra("pcursor") is None:
                urlPack.setForm(self.form_first.format(user_id=msg.msgData))
            else:
                urlPack.setForm(self.form_next.format(user_id=msg.msgData, pcursor=msg.getExtra("pcursor")))
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
        log.debug("KuaiShouAuthorVideosProvider next: {}".format(urlPack))
        return urlPack
    
    def end(self):
        return False
    
    def check(self):
        return False
        
    
    
