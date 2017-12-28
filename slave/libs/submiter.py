# -*- coding=utf-8 -*-
from __future__ import print_function
'''
Created on 2017年11月29日

@author: yangyh
'''
import requests
import json
import time
from libs import log
from libs import conftool


SUBMIT_HOST = conftool.cf.get('sender_callback', 'host')
SUBMIT_PORT = conftool.cf.get('sender_callback', 'port')

def addMp4Job(itemKey, priority=10000):
    itemKey = itemKey.strip()
    for _ in range(3):
        try:
            params = {"cmd": "add", "_key_": itemKey, "priority": priority}
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            resp = requests.get("http://{}/job".format(host), params=params)
            return json.loads(resp.text)
        except Exception as e:
            log.warning("addMp4Job", e)
            time.sleep(1)
            pass
    log.fatal("submiter.addMp4Job fail")
    return False


def addHuoshanAuthorDetailJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=PipeHuoshanAuthorDetail&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addHuoshanAuthorDetailJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addHuoshanVideosJob_error:".format(uid))
    return False


def addKuaishouVideosJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=KuaiShouAuthorVideosPipeCspub&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addKuaishouVideosJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addKuaishouVideosJob_error:".format(uid))
    return False


def addHuoshanVideosJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=PipeHuoshanAuthorVideos&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addHuoshanVideosJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addHuoshanVideosJob_error:".format(uid))
    return False


def addDouyinVideosJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=DouyinAuthorVideoPipeCspub&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addDouyinVideosJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addDouyinVideosJob_error:".format(uid))
    return False

addDouyinAuthorVideosJob = addDouyinVideosJob

def addDouyinAuthorDetailJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=DouyinAuthorDetailPipeCspub&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addDouyinAuthorDetailJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addDouyinVideosJob_error:".format(uid))
    return False

def addKuaishouAuthorDetailJob(uid, priority=10):
    for _ in range(3):
        try:
            uid = int(uid)
            host = conftool.randomChoice(SUBMIT_HOST, SUBMIT_PORT)
            r = requests.get("http://{}/spider/add?priority={}&pipe=KuaiShouAuthorDetailPipeCspub&msg_type=AUTHOR&msg_data={}".format(host, priority, uid))
            log.notice("addKuaishouVideosJob:{}".format(uid, r.text))
            return True
        except:
            pass
    log.fatal("addKuaishouAuthorDetailJob_error:{}".format(uid))
    return False

    
