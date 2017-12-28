# -*- coding=UTF-8 -*-
'''
Created on 2017年5月25日

@author: yangyh
'''
import ConfigParser
import os
import socket
import random
hostname = socket.gethostname()

cf = ConfigParser.ConfigParser()
root = os.path.dirname(os.path.abspath(__file__)) + "/../"
root = os.path.abspath(root)
throw = True
if hostname.find("yangyhdeMacBook") >= 0:
    cur_env = "mac"
    throw = False
else:
    cur_env = open(root + "/sconf/env", 'r').read().strip()

is_debug_env = cur_env != 'online'
is_online = cur_env == 'online'

conf_dir = root + "/sconf/" + cur_env + "/rmb.conf"    
cf.read(conf_dir)

def parseHostLines(lines):
    return ([x.strip() for x in lines.split("\n") if len(x.strip()) > 0])

def randomChoice(hosts, ports):
    arrHosts = [x.strip() for x in hosts.split(",")]
    arrPorts = [x.strip() for x in ports.split(",")]
    return random.choice(arrHosts) + ":" + random.choice(arrPorts)
    
    
