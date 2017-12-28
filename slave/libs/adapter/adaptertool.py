# -*- coding=utf-8 -*-
'''
Created on 2017年11月30日

@author: yangyh
'''
from libs.db import dbtools

adapters = {} 

F_NUM_FANS = "numFans"
F_NUM_FOLLOWING = "numFollow"
F_UID  = "user3rdId"


def getUid(resp):
    if '_user3rdId_' in resp:
        return resp['_user3rdId_']
    elif 'user_id' in resp:
        return resp['user_id']
    elif 'author_user_id' in resp:
        return resp['author_user_id']
    elif 'uid' in resp:
        return resp['uid']
    raise ValueError("could not get uid")

def transform(key, data):
    global adapters
    itemType, provider, _, _ = dbtools.get_key_info(key)
    if provider not in adapters:
        adapters[provider] = __import__('libs.adapter.adapter_' + provider, fromlist=["libs.adapter"])
    adapter = adapters[provider]
    if itemType == "VIDEO":
        resp = adapter.transfromVideoDetail(data)
        return resp
    if itemType == "AUTHOR":
        resp = adapter.transformAuthorDetail(data)
        return resp
    if itemType == "TOPIC":
        resp = adapter.transformTopicDetail(data)
        return resp
    return False
    
