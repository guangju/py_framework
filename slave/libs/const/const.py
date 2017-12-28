# -*- coding=utf-8 -*-
"""
Created on 2017年11月25日

@author: yangyh
"""

DATA_TYPE_AUTHOR = "AUTHOR"
DATA_TYPE_TOPIC = "TOPIC"
DATA_TYPE_VIDEO = "VIDEO"

CRAWL_STATUS_OK = 1
CRAWL_STATUS_GIVEUP = 2
CRAWL_STATUS_NEEDRETRY = 3
CRAWL_STATUS_SUBMIT = 200
CRAWL_STATUS_ERROR = 500
CRAWL_STATUS_MISS_AUTHOR = 404
#待高优抓取
CRAWL_STATUS_HIGH_PRIORITY = 110

ARR_FAILED = [0, 
              CRAWL_STATUS_GIVEUP,
              CRAWL_STATUS_NEEDRETRY,
              CRAWL_STATUS_ERROR,
              CRAWL_STATUS_MISS_AUTHOR,
              CRAWL_STATUS_HIGH_PRIORITY
              ]

g_map = {
    DATA_TYPE_TOPIC: "m_topic",
    DATA_TYPE_VIDEO: "m_video",
    DATA_TYPE_AUTHOR: "m_author"
    }

def getTable(key):
    key = key.strip()
    if key.find("-") > 0:
        key = key.split("-")[0]
    return g_map[key]
