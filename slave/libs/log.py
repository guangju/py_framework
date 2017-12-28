# -*- coding=utf-8 -*-
################################################################################
#
# attrprovider
# @author dongliqiang@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
#
################################################################################
import sys
reload(sys)
#func = getattr(sys, "setdefaultencoding")
#func('utf-8')
sys.setdefaultencoding('utf-8')  # @UndefinedVariable

import codecs
import logging.handlers
import os
import urllib
import traceback
import time
import tornado.log

DEBUG = True
LOG_DIR = "./log/"
MODULE = "vuserver"
FOTMAT = "%(levelname)s: %(asctime)s %(f_module)s \
[filename=%(f_filename)s lineno=%(f_lineno)d \
process=%(process)d thread=%(thread)d \
thread_name=%(threadName)s created=%(created)f msecs=%(msecs)d %(message)s]"
ACCESS_FORMAT = "%(asctime)s %(module)s %(process)d [%(message)s]"
LEVEL_NOTICE = 21
LEVEL_FATAL = 41
logging.addLevelName(LEVEL_NOTICE, "NOTICE")
logging.addLevelName(LEVEL_FATAL, "FATAL")

"""
app_log is suitable for human reading.

app_omg_log is suitable for statistics and monitor,
each request ONLY has one line log.

"""
app_log = logging.getLogger(MODULE + ".log.app")
app_omp_log = logging.getLogger(MODULE + ".log.omp")
app_log_wf = logging.getLogger(MODULE + ".log.app_wf")
app_omp_log_wf = logging.getLogger(MODULE + ".log.omp_wf")


def _get_current_trackback():
    """
    get current traceback

    traceback is wrong in the log file.
    so we should find it by myself.
    """
    traces = traceback.extract_stack()
    for i, trace in enumerate(traces):
        if trace[0].endswith("huskar/log.py") or\
                trace[0].endswith("huskar/log.pyc") or\
                trace[0].endswith("huskar/log.pyo"):
            if trace[2] in [
                "debug", "notice", "warning", "fatal"]:
                return traces[i - 1]
    # can not find any current trace.
    # return the default
    return traces[0]


class MultiProcessSafeDailyRotatingFileHandler(logging.handlers.BaseRotatingHandler):
    """Similar with `logging.TimedRotatingFileHandler`, while this one is
    - Multi process safe
    - Rotate at midnight only
    - Utc not supported
    """
    """

    """
    def __init__(self, filename, encoding=None, delay=False, utc=False, **kwargs):  # @UnusedVariable
        self.utc = utc
        self.suffix = "%Y%m%d%H"
        self.baseFilename = filename
        self.currentFileName = self._compute_fn()
        logging.handlers.BaseRotatingHandler.__init__(self, filename, 'a', encoding, delay)

    def shouldRollover(self, record):  # @UnusedVariable
        if self.currentFileName != self._compute_fn():
            return True
        return False

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        self.currentFileName = self._compute_fn()

    def _compute_fn(self):
        return self.baseFilename + "." + time.strftime(self.suffix, time.localtime())

    def _open(self):
        if self.encoding is None:
            stream = open(self.currentFileName, self.mode)
        else:
            stream = codecs.open(self.currentFileName, self.mode, self.encoding)
        # simulate file name structure of `logging.TimedRotatingFileHandler`
        if os.path.exists(self.baseFilename):
            try:
                os.remove(self.baseFilename)
            except OSError:
                pass
        try:
            os.symlink(os.path.abspath(self.currentFileName), os.path.abspath(self.baseFilename))
        except OSError:
            pass
        return stream

"""
包装后的logger 会默认打印当前的trace
所有的日志 都会是log的行号 和 stack 信息
所以这里都统一替换一下

Args:
    parser (TYPE): Description
"""


class FilenameFilter(logging.Filter):
    """Summary
    FilenameFilter
    """

    def filter(self, record):
        """Summary
        filter
        """
        record.f_filename = os.path.abspath(_get_current_trackback()[0])
        return True


class ModuleFilter(logging.Filter):
    """Summary
    ModuleFilter
    """

    def filter(self, record):
        """Summary
        filter
        """
        record.f_module = _get_current_trackback()[2]
        return True


class LinenoFilter(logging.Filter):
    """Summary
    LinenoFilter
    """

    def filter(self, record):
        """Summary
        filter
        """
        record.f_lineno = _get_current_trackback()[1]
        return True


def init(log_dir=LOG_DIR, module=MODULE, debug=True):
    """Summary
    feed log init.

    Attribute:
        log_dir: log dir
        module: log name
        debug:
    """
    global DEBUG
    DEBUG = debug
    try:
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
    except:
        pass

    if debug is True:
        app_log.setLevel(logging.DEBUG)
        app_omp_log.setLevel(logging.DEBUG)
        app_log_wf.setLevel(logging.DEBUG)
        app_omp_log_wf.setLevel(logging.DEBUG)
    else:
        app_log.setLevel(logging.INFO)
        app_omp_log.setLevel(logging.INFO)
        app_log_wf.setLevel(logging.WARNING)
        app_omp_log_wf.setLevel(logging.WARNING)

    formatter = logging.Formatter(FOTMAT)
    filename_filter = FilenameFilter()
    module_filter = ModuleFilter()
    lineno_filter = LinenoFilter()

    # app log
    app_log_hd = MultiProcessSafeDailyRotatingFileHandler(
        log_dir + "/" + module + ".log")
    #app_log_hd.setFormatter(formatter)
    app_log.addHandler(app_log_hd)
    #app_log.addFilter(filename_filter)
    #app_log.addFilter(module_filter)
    #app_log.addFilter(lineno_filter)
    app_log.propagate = False


    # access log
    fh_access = logging.handlers.TimedRotatingFileHandler(
        log_dir + "/" + module + ".access.log", "H", 1, 24 * 7)
    #fh_access.suffix = "%Y%m%d%H"
    #fh_access.setFormatter(logging.Formatter(ACCESS_FORMAT))
    tornado.log.access_log.addHandler(fh_access)
    tornado.log.access_log.setLevel(logging.DEBUG)
    tornado.log.access_log.propagate = False

    # app warning fatal log
    app_log_wf_hd = MultiProcessSafeDailyRotatingFileHandler(
        log_dir + "/" + module + ".log.wf")
    #app_log_wf_hd.setFormatter(formatter)
    app_log_wf.addHandler(app_log_wf_hd)
    #app_log_wf.addFilter(filename_filter)
    #app_log_wf.addFilter(module_filter)
    #app_log_wf.addFilter(lineno_filter)
    app_log_wf.propagate = False

    # app omp log
    app_omp_log_hd = MultiProcessSafeDailyRotatingFileHandler(
        log_dir + "/" + module + ".new.log")
    #app_omp_log_hd.setFormatter(formatter)
    app_omp_log.addHandler(app_omp_log_hd)
    #app_omp_log.addFilter(filename_filter)
    #app_omp_log.addFilter(module_filter)
    #app_omp_log.addFilter(lineno_filter)
    app_omp_log.propagate = False

    # app omp warning fatal log
    app_omp_log_wf_hd = MultiProcessSafeDailyRotatingFileHandler(
        log_dir + "/" + module + ".new.log.wf")
    #app_omp_log_wf_hd.suffix = "%Y%m%d%H"
    #app_omp_log_wf_hd.setFormatter(formatter)
    app_omp_log_wf.addHandler(app_omp_log_wf_hd)
    #app_omp_log_wf.addFilter(filename_filter)
    #app_omp_log_wf.addFilter(module_filter)
    #app_omp_log_wf.addFilter(lineno_filter)
    app_omp_log_wf.propagate = False


def _urlencode(value):
    """Summary
    _urlencode
    """
    if isinstance(value, unicode):
        return urllib.quote_plus(value.encode('utf8'))
    return urllib.quote_plus(value)


def debug(*args, **kwargs):
    """Summary
    add a DEBUG log
    """
    if not DEBUG:
        return
    logstr = _gen_log_str(*args, **kwargs)
    #print("[DEBUG {}] {}".format(time.asctime(time.localtime(time.time())), logstr))
    fileName = sys._getframe().f_back.f_code.co_filename.split("/") #获取文件名
    funcName = sys._getframe().f_back.f_code.co_name #获取调用函数名
    lineNumber = sys._getframe().f_back.f_lineno     #获取行号
    logstr = "[{} {}:{}:{}] {}".format(time.strftime("%Y/%m/%d-%X",
                                                    time.localtime()),
                                                    fileName[-1],
                                                    funcName,
                                                    lineNumber,
                                                    logstr)
    print("[DEBUG] {}".format(logstr))
    app_log.debug(logstr)


def dict2str(d):
    """
    parse dict to string
    """
    return " ".join(["[{}={}]".format(k, v) for k, v in d.iteritems()])


def _gen_log_str(*args, **kwargs):
    arr = []
    if type(args) == tuple:
        args = list(args)
        if len(args) == 0:
            args = None
        elif len(args) == 1:
            args = args[0]
        else:
            args = "=".join([str(x) for x in args]) 
            
    if type(args) == dict:
        arr.append(dict2str(args))
    elif args is not None:
        arr.append(str(args))
    else:
        arr.append('None')
        
    if type(kwargs) == dict:
        arr.append(dict2str(kwargs))
    else:
        arr.append(str(kwargs))
    return " ".join(arr)
    

def notice(*args, **kwargs):
    """Summary
    add a NOTICE log
    """
    logstr = _gen_log_str(*args, **kwargs)
    fileName = sys._getframe().f_back.f_code.co_filename.split("/") #获取文件名
    funcName = sys._getframe().f_back.f_code.co_name #获取调用函数名
    lineNumber = sys._getframe().f_back.f_lineno     #获取行号
    logstr = "[{} {}:{}:{}] {}".format(time.strftime("%Y/%m/%d-%X",
                                                    time.localtime()),
                                                    fileName[-1],
                                                    funcName,
                                                    lineNumber,
                                                    logstr)
    app_log.log(LEVEL_NOTICE, logstr)
    app_omp_log.log(LEVEL_NOTICE, logstr)


def warning(*args, **kwargs):
    """Summary
    add a WARNING log
    """
    logstr = _gen_log_str(*args, **kwargs)
    fileName = sys._getframe().f_back.f_code.co_filename.split("/") #获取文件名
    funcName = sys._getframe().f_back.f_code.co_name #获取调用函数名
    lineNumber = sys._getframe().f_back.f_lineno     #获取行号
    logstr = "[{} {}:{}:{}] {}".format(time.strftime("%Y/%m/%d-%X",
                                                    time.localtime()),
                                                    fileName[-1],
                                                    funcName,
                                                    lineNumber,
                                                    logstr)
    app_log_wf.warning(logstr)
    app_omp_log_wf.warning(logstr)
    print("[WARNING] {}".format(logstr))


def fatal(*args, **kwargs):
    """Summary
    add a FATAL log
    """
    logstr = _gen_log_str(*args, **kwargs)
    fileName = sys._getframe().f_back.f_code.co_filename.split("/") #获取文件名
    funcName = sys._getframe().f_back.f_code.co_name #获取调用函数名
    lineNumber = sys._getframe().f_back.f_lineno     #获取行号
    logstr = "[{} {}:{}:{}] {}".format(time.strftime("%Y/%m/%d-%X",
                                                    time.localtime()),
                                                    fileName[-1],
                                                    funcName,
                                                    lineNumber,
                                                    logstr)
    app_log_wf.log(LEVEL_FATAL, logstr)
    app_omp_log_wf.log(LEVEL_FATAL, logstr)
    print("[FATAL] {}".format(logstr))


if __name__ == '__main__':

    init()
    notice("aa", "bb")
    notice("aa", "cc", "dd")
    notice({"a": "b", "c": "d"})
    notice(a="123", b="dafd", c=344)
    notice("port", "start 8999")
    notice("aaa", "bbfdl;akr3i2u53214%#&^%&^%#$@Wgfdlk\
        sjf红额外哦IQ缴费机啊街坊大姐唉算了；就发")

    warning("get_error_func", "fdaljfda辅导课辣椒水放假的撒娇 ")
    fatal("get_user_info", "failed to get user infomation")

    # test unicode
    notice("fdafda", u"地方大快速链接++++")

    # test dict
    notice({"a": "b", "c": "d"})