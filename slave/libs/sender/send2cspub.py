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
version
"""
from __future__ import division, print_function, unicode_literals
from libs.adapter import adaptertool as ap
from libs.spider import cspubutil
import requests
import time
import json
import traceback
from libs import log
from libs import error
from libs import conftool
from libs import util
from libs.db import mongo
from libs.db import dbtools
from libs import statistics
from libs.const import const
from libs.sender.send2socket import Sender2Socket


CALLBACK_HOST = conftool.cf.get('sender_callback', 'host')
CALLBACK_PORT = conftool.cf.get('sender_callback', 'port')

MIMOD_HOST = conftool.cf.get('mimod_server', 'host')
MIMOD_PORT = conftool.cf.get('mimod_server', 'port')

#server_spider queue
DEST_HOST = '10.95.28.43'
DEST_PORT = 8040

#useless
global_dest_host = 'm1-pslx-shylock5.m1'
global_dest_port = 14000


db = mongo.DB()
stat = statistics.Statistics()

def send2cspub_by_key(key):
    key = key.strip()
    req_url = 'http://' + conftool.randomChoice(CALLBACK_HOST, CALLBACK_PORT) + '/item/info' 
    trespassing_field = None
    for _ in range(3):
        try:
            resp = requests.get(req_url, params={"key": key})
            data = resp.text
            data = json.loads(data)['data']
            if "target_url" in data:
                trespassing_field = data
                break
        except Exception as e:
            log.warning("resp_error:{}, {}".format(e, key))
            continue
    if trespassing_field is None:
        log.fatal("get_item_info_error, key:{}".format(key))
        return False
    ok = _send2cspub_(key, trespassing_field)
    log.notice("finish sending: {}, bypass: {}, result: {}".format(key, trespassing_field, ok))

def getLoc(target_url):  
    return "http://www.internal.video.baidu.com/{}.html".format(util.md5(target_url))
        
def _send2cspub_(key, info):   
    csdata = {}
    csdata['dest_port'] = 10122
    csdata['priority'] = 20
    csdata['dest_host'] = 'yq01-spi-linkfound2.yq01.baidu.com'
    csdata['user'] = '1'
    info['source_type'] = 1
    csdata['user_key'] = 'midway_island'
    csdata['target_url'] = info['target_url'].replace('https', 'http')
    csdata['trespassing_field'] = info
    failed = cspubutil.send2cspub([csdata])
    if len(failed) == 0:
        value = {}
        value["_crawl_"] = const.CRAWL_STATUS_OK
        value["_utime_"] = int(time.time())
        db.updateByKey(const.getTable(key), key, value)
        log.debug("sending directly key={}, url={}".format(key, "http://www.internal.video.baidu.com/{}.html".format(util.md5(csdata['target_url']))))
        return True
    log.fatal("send2cspub_directly_error")
    return False
    

def patchMicroVideoExt(info):
    try:
        i = json.loads(info['microVideoExt'])
        i['flag'] = 2
        info['microVideoExt'] = json.dumps(i)
    except:
        log.fatal("patchMicroVideoExt_error")
        pass

def send2cspub(mode='queue', req_url = None, dest_host = None, dest_port = None):
    """
        main worker
    """
    req_url = 'http://' + conftool.randomChoice(CALLBACK_HOST, CALLBACK_PORT) + '/job?cmd=get' if req_url is None else req_url
    dest_host = DEST_HOST if dest_host is None else dest_host
    dest_port = DEST_PORT if dest_port is None else dest_port
    try:
        resp = requests.get(req_url, timeout=30)
        data = resp.text
        data = json.loads(data)
        if data['data']['_key_'] is False:
            log.warning("queue empty")
            return False
        if data['data'].get('_crawl_') == const.CRAWL_STATUS_OK:
            return True
        itemKey = data['data']['_key_']
    except Exception as e:
        log.fatal("get queue error", e)
        raise error.BaseError(errno=500, errmsg="get queue error")

    if 'errno' in data and data['errno'] == 0 and 'data' in data:
        try:
            info = ap.transform(itemKey, data['data'])
            #bypass data version
            info['_v'] = 2
        except Exception as e:
            traceback.print_exc()
            log.fatal("transfromVideoDetail_error:_key_={}, data={}, err={}".format(itemKey, data, e))
            return {'code': 2, 'data': 'send fail'}
        if info['_crawl_'] == const.CRAWL_STATUS_OK:
            return {'code': 1, 'data': 'crawled data'}
    
        if mode == 'queue':
            sender = Sender2Socket(dest_host, dest_port)
            csdata = {}
            csdata['dest_port'] = global_dest_port
            csdata['dest_host'] = global_dest_host
            csdata['user'] = '1'
            csdata['user_key'] = 'midway_island'
            csdata['target_url'] = info['target_url'].replace('https', 'http')
            csdata['trespassing_field'] = json.loads(json.dumps(info))
            ret = sender.send([json.dumps(csdata)])
        elif mode == 'direct':
            csdata = {}
            mimodAddr = conftool.randomChoice(MIMOD_HOST, MIMOD_PORT).split(":")
            csdata['dest_port'] = int(mimodAddr[1])
            csdata['dest_host'] = mimodAddr[0]
            csdata['user'] = '1'
            info['source_type'] = 1
            csdata['user_key'] = 'midway_island'
            csdata['target_url'] = info['target_url'].replace('https', 'http')
            patchMicroVideoExt(info)
            csdata['trespassing_field'] = info
            failed = cspubutil.send2cspub([csdata])
            if len(failed) == 0:
                value = {}
                value["_crawl_"] = 1
                value["_utime_"] = int(time.time())
                resp = db.updateByKey(const.getTable(itemKey), itemKey, value)
                _, provider, _, _ = dbtools.get_key_info(itemKey)
                stat.incrProviderSend2cspub(provider)
                ret = True
            else:
                ret = False
                log.fatal("send2cspub_directly_error: {}".format(itemKey))
            
            if ret:
                log.debug("sending directly key={}, url={}, topic={}".format(itemKey, getLoc(csdata['target_url']), info.get('microVideoTopic')))
            
        else:
            raise ValueError("invalid mode")
        if ret:
            log.notice("sending {}".format(getLoc(csdata['target_url'])))
            return {'code': 0, 'data': csdata}
        log.fatal("send2cspub_fail", csdata)
        return {'code': 1, 'data': 'send fail'}
    else:
        log.fatal("get queue error", data)
        raise error.BaseError(errno=500, errmsg="get queue error")
