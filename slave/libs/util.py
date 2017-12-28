# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年6月21日 下午2:59:31
#
################################################################################
"""
工具类
"""
from __future__ import division, print_function, unicode_literals
import os
import urllib2
import urlparse
import sys
import json
import time
import datetime
import random
import hashlib
import gzip
import StringIO
import requests
import shutil
import pytz
import re
import traceback
from urllib import urlencode
from os.path import basename

from libs import log
    

def date2ts(s):
    return int(time.mktime(datetime.datetime.strptime(str(s), "%Y%m%d").timetuple()))

def get_locid(loc):
    """
    extract locid
    loc:http://www.internal.video.baidu.com/6f00686e70de686741e71a072b6ef864.html
    return: 6f00686e70de686741e71a072b6ef864
    """
    return os.path.splitext(os.path.basename(loc))[0]


def get_loc_url(locid):
    """
    6f00686e70de686741e71a072b6ef864 => http://www.internal.video.baidu.com/6f00686e70de686741e71a072b6ef864.html
    """
    return "http://www.internal.video.baidu.com/{}.html".format(locid)

def is_valid_loc(s):
    """
    是否有效的loc
    """
    m = re.match('^http://www.internal.video.baidu.com/[a-z0-9]{32}.html$', s)
    return m is not None


def safe_cast(val, to_type, default=None):
    """
        method
    """     
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default


def safe_json_decode(s, default=None):
    """  
    """
    if s is None:
        return None
    try:
        return json.loads(s)
    except:
        traceback.print_exc()
        log.warning("safe_json_decode", str(s)[:100])
        return default


def get_32bit_locid(s):
    """
    extract id from loc url
    """
    return os.path.splitext(os.path.basename(s))[0]


def load_file_asarray(file_path):
    """
    load file as array
    """
    arr = []
    with open(file_path, "r") as f:
        for s in f:
            s = s.strip()
            if len(s) > 0:
                arr.append(s)
    return arr
           

def load_file_asdict(file_path, keyIndex, seperator="\t", limit=-1):
    """
    key => array()
    """
    d = {}
    cnt = 0
    with open(file_path, "r") as f:
        for s in f:
            s = s.strip()
            if len(s) > 0:
                a = s.split(seperator)
                if a[keyIndex] not in d:
                    d[a[keyIndex]] = [a]
                else:
                    d[a[keyIndex]].append(a)
            cnt += 1
            if limit > 0 and cnt >= limit:
                break
    return d
            

def is_number(s):
    """
        method
    """     
    try:
        float(s)
        return True
    except ValueError:
        return False


def gzip_compress(buf):
    """
        method
    """ 
    out = StringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(buf)
    return out.getvalue()

def gzip_decompress(buf):
    """
        method
    """ 
    obj = StringIO.StringIO(buf)
    with gzip.GzipFile(fileobj=obj) as f:
        result = f.read()
    return result

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def gen_logid():
    return str(current_timestamp()) + str(random.randint(100000, 999999))

def md5(src):
    """
        method
    """ 
    m2 = hashlib.md5()   
    m2.update(src)   
    return m2.hexdigest()

def download_file_to_local(url, dest, **argus):
    """
        method
    """ 
    #TODO: ADD RETRY
    try:
        resp = requests.get(url, stream = True)
        headers = resp.headers
        length = safe_cast(headers.get('Content-Length'), int, 0)
        if length == 0:
            sys.stderr.write("[ERROR]error get Content-Length:{}\n".format(url))
            return False
        #TODO: JUDGE ZERO
        default_max_size = 524288000
        if length > default_max_size:
            sys.stderr.write("[ERROR]len_exceed:%d,%s\n" % (length, url))
            return False
        if length > safe_cast(argus.get("max_size"), int, default_max_size):
            return False
        with open(dest, "wb") as myfile:
            shutil.copyfileobj(resp.raw, myfile)
        return True
    except Exception as ex:
        #default retry 2 times
        retry = safe_cast(argus.get('retry'), int, 2)
        if retry > 0:
            argus["retry"] = retry - 1
            time.sleep(0.1)
            return download_file_to_local(url, dest, **argus)
        else:
            raise ex
        

def get_http_content(url, **kwargs):
    try:
        timeout = kwargs.get('timeout', 120)
        if timeout is None:
            timeout = 120
        res = urllib2.urlopen(url, timeout=timeout)
        meta = res.info()
        if len(meta.getheaders("Content-Length")) > 0:
            length = int(meta.getheaders("Content-Length")[0])
            if length > 268435456:
                safe_print_err("[ERROR]len_exceed:%d,%s" % (length, url))
                res.close()
                return None
        txt = res.read()
        res.close()
        return txt
    except Exception as e:
        traceback.print_exc()
        safe_print_err("[ERROR]get_http_content_error:%s,%s" % (str(e), url))
        return None

def extract_file_path(url):
    url=urlparse.urlparse(url)
    return basename(url.path)

def raise_list_key(input_list, key):
    output_dict = {}
    for i in input_list:
        output_dict[i[key]] = i 
    return output_dict

def replace_url_to_normal_filename(url):
    if url is not None:
        url = url.replace(":", "_")
        url = url.replace("/", "_")
        url = url.replace(".", "_")
        url = url.replace("|", "_")
    return url


def merge_set(x1, y1, x2, y2):
    if x2 < x1:
        t1,t2 = x1,y1
        x1,y1 = x2,y2
        x2,y2 = t1,t2
    if x2 >= x1 and x2 <= y1:
        if y2 >= y1:
            return [[x1, y2]]
        return [[x1, y1]]
    return [[x1, y1], [x2, y2]]


def get_log_str(log_key, **kwargs):
    """
    get log string
    """
    s = "[type={}]".format(log_key)
    for k in kwargs:
        s += " [{}={}]".format(k, kwargs[k])
    return s
    

def get_video_meta(vid):
    qs = urlencode({"ids" : vid})
    resp = get_http_content('http://tc-orp-app0496.tc:8260/video/quality/meta' + "?" + qs)
    return safe_json_decode(resp)


def current_timestamp():
    return int(time.time() - time.timezone)


def timestamp2datetime(timestamp):
    """时间戳转为日期
    """
    return datetime.datetime.fromtimestamp(int(timestamp), 
                pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

def current_datetime():
    return datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y%m%d-%H%M%S')

def current_date(incDate=0):
    return datetime.datetime.fromtimestamp(int(time.time() + incDate * 86400)).strftime('%Y%m%d')

def current_hour(incHour=0):
    return datetime.datetime.fromtimestamp(int(time.time() + incHour * 3600)).strftime('%Y%m%d-%H')



def get_video_related(title):
    qs = urlencode({"title" : title})
    ret = get_http_content('http://sv.baidu.com/videoui/data/videorelate' + "?" + qs)
    if ret is None:
        return None
    return json.loads(ret)
   
            
def str2bool(v):
    if v is None:
        return False
    if type(v) == list:
        if len(v) == 0:
            return False
        v = v[0]
    v = str(v)
    return v.lower() in ("yes", "true", "t", "1")

def is_recent_hour(time_stamp, hour = 1):
    """
        is recently
    """
    time_stamp = int(time_stamp)
    return abs(time.time() - time.timezone - time_stamp) < 86400 * hour 


def get_duration_from_brief(image_folder):
    """
    get duration from brief
    """
    try:
        with open(image_folder + "/brief.txt", "r") as f:
            cont = f.read()
            cont = cont.split()
            return float(cont[3])
    except:
        return 0


def safe_unlink(path):
    """
    delete file without warning
    """
    if path is None:
        return
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)
    except:
        pass


def extract_mp3(ffmpeg, input_path, output_path): 
    """
        audio extract
    """
    os.system("{} -i {} -q:a 0 -map a -y {}".format(ffmpeg, input_path, output_path))


def detect_ncpus():
    """
    Detects the number of effective CPUs in the system
    """
    #for Linux, Unix and MacOS
    if hasattr(os, "sysconf"):
        if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
            #Linux and Unix
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
        else:
            #MacOS X
            return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    #for Windows
    if "NUMBER_OF_PROCESSORS" in os.environ:
        ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
        if ncpus > 0:
            return ncpus
    #return the default value
    return 1

def dict_incr_key_value(dictorary, key, initval=1):
    """
        词典中的value加1
    """
    if key in dictorary:
        dictorary[key] += 1
    else:
        dictorary[key] = initval
        
   
def dict_incr_key_set(dictorary, key, val):
    """
        词典中的value是个set, 添加一个元素
    """
    if key in dictorary:
        dictorary[key].add(val)
    else:
        dictorary[key] = set([val])
        

def recursive_walk(folder):
    """
    遍历目录
    """
    for folderName, subfolders, filenames in os.walk(folder):
        if subfolders:
            for subfolder in subfolders:
                recursive_walk(subfolder)
        #print('\nFolder: ' + folderName + '\n')
        for filename in filenames:
            #print(filename + '\n')
            yield folderName + "/" + filename
     
            
def raise_runtime_error(msg):
    """
    log and raise
    """
    #TODO: get context
    traceback.print_exc()
    log.warning(msg)
    raise RuntimeError(msg)


def httplog(app_id, param):
    """
    http log
    """
    if type(param) != dict:
        return -1
    try:
        cur = time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time()))
        res = urllib2.urlopen("http://10.199.74.46:8080/dataximporter/api/log?app_id=" + app_id + "&date=" + cur + "&type=rmb&" + urlencode(param), timeout=10)
        res.read()
        res.close()
        return 1
    except:
        return 0


def safe_print(content):
    """
    thread safe print
    """
    print("{0}".format(content))


def safe_print_err(content):
    """
    thread safe print to stderr
    """
    print >> sys.stderr, "{0}\n".format(content),


def safe_append_file(mutex, txtFile, line):
    if mutex is not None:
        mutex.acquire(True)
    with open(txtFile, 'a') as f:
        f.write("{}\n".format(line))
    if mutex is not None:
        mutex.release()
        
def qs_to_dict(qs):
    final_dict = dict()
    for item in qs.split("&"):
        final_dict[item.split("=")[0]] = item.split("=")[1]
    return final_dict
        
        
def retry(fun, *param, **kwargs):
    CNT = 3
    for i in range(3):
        try:
            resp = fun(*param, **kwargs)
            return resp
        except Exception as e:
            traceback.print_exc()
            log.fatal("{}, error: {}, retrying {}".format(fun, e, i))
            if i < CNT - 1:
                time.sleep(1)
    return False
            
