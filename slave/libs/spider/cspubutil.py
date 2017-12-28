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
Created on 2017年11月27日

@author: yangyh
"""

import json
import socket
import time
import random
import traceback
from libs import log, conftool, util
from libs import statistics

try:
    from libs.common import mcpack
    from libs.common.nshead import nshead
except Exception as e:
    traceback.print_exc()
    if conftool.throw:
        raise e
    
    

#正式环境
CSPUB_HOST = "10.86.237.18"
#测试环境
#CSPUB_HOST = "10.58.78.12"
CSPUB_PORT = 7205

CALLBACK_HOST = str(conftool.cf.get("cspub", "callback_host"))
CALLBACK_PORT = int(conftool.cf.get("cspub", "callback_port"))

CALLBACK_HOST = conftool.parseHostLines(CALLBACK_HOST)

stat = statistics.Statistics()

def send2cspub(data_list, host=CSPUB_HOST, port=CSPUB_PORT):
    if not data_list:
        return []
    
    mcpack.set_default_version(2)
    server = (host, port)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except Exception as err:
        log.fatal('Creating socket error:{}'.format(err))
        return data_list
    try:
        sock.connect(server)
    except Exception as err:
        log.fatal('Connecting to server error:{}'.format(err))
        return data_list
    
    data_failed = []
    for line in data_list:
        json_string = line
        if type(json_string) != dict:
            send_dict = json.loads(json_string)
        else:
            send_dict = json_string
        send_pack = mcpack.dumps(send_dict)
        try:
            nshead.nshead_write(sock, send_pack)
        except Exception as err:
            log.fatal('Sending data error:{}'.format(err))
            data_failed.append(line)

    sock.close()
    return data_failed


def patch(json_data, bypass=None, urlPack=None, destHost=None, destPort=None):
    json_data['user'] = '1'
    json_data['user_key'] = 'midway_island'
    if destHost is None:
        destHost = random.choice(CALLBACK_HOST)
    if destPort is None:
        destPort = CALLBACK_PORT
    json_data['dest_host'] = destHost
    json_data['dest_port'] = destPort
    if urlPack is not None:
        if 0 <= urlPack.priority <= 32:
            json_data['priority'] = urlPack.priority
        else:
            json_data['priority'] = urlPack.getKey('priority', 0)
    if bypass is not None and type(bypass) == dict:
        bypass['_log_id'] = time.time()
        bypass['_cb'] = destHost + ":" + str(destPort)
        json_data['trespassing_field'] = mcpack.OBJ(bypass)
        currentHour = util.current_hour()
        stat.incrRedis("cspub:sent:{}:{}".format(currentHour, bypass.get('pipe')), expire=86400)
        
        
    
