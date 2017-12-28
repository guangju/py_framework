# -*- coding=utf-8 -*-
'''
Created on 2017年5月21日

@author: yangyh
'''

import tornado.web
import random
import sys
import traceback
import pprint
from libs import error
from libs import log
from libs import util


from concurrent.futures import ThreadPoolExecutor 
from tornado.concurrent import run_on_executor

class BaseRmbHandler(tornado.web.RequestHandler):
    '''
    classdocs
    '''
    executor = ThreadPoolExecutor(512)
    
    errno = 0
    errmsg = ""
    logid = ""
    request_data = {}
    response_data = {}
    
    def __init__(self, application, request, **kwargs):
        super(BaseRmbHandler, self).__init__(application, request, **kwargs)
        self._init()
    
    def initialize(self, **kwarg):
        """
            initialize
        """
        pass
    
    def _init(self):
        self.request_data = self._parse_request()
        self.logid = self._generate_logid()
        self.init()
    
    def init(self):
        pass
    
    def _parse_request(self):
        """
        parse the request body.

        you can override this method to parse your special protocol
        """
        if len(self.request.body) > 0:
            try:
                return tornado.escape.json_decode(self.request.body)
            except Exception:
                #Not Json, Using Form data
                return self.request.arguments
        else:
            return self.request.arguments
    
    def _generate_logid(self):
        """
        generate a logid for the request.

        you can override this method if you have the special protocol
        """
        if "logid" in self.request_data:
            return self.request_data["logid"]
        logid = self.get_argument("logid", random.randint(10 ** (len(str(sys.maxint)) - 1), sys.maxint))
        return logid
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):  # @UnusedVariable
        yield self._work()
        if self._finished:
            return
        self.finish()
        
    @run_on_executor    
    def _work(self):
        """
        
        """
        
        try:
            self.work()
            self._render_result(self.errno, self.errmsg, self.response_data)
        except error.BaseError as e:
            self._render_result(e.errno, e.errmsg, {})
            warning = {
                "uri": self.request.uri,
                "logid": self.logid,
                "errno": e.errno,
                "errmsg": e.errmsg,
                "args" : str(e.args),
                "trace": traceback.format_exc(),
                "ex_type": type(e)
            }
            log.warning(warning)
            sys.stderr.write(pprint.pformat(warning))
        except Exception,e:
            errno = error.ERRNO_UNKNOWN
            self._render_result(errno, str(e), "")
            warning = {
                "uri" : self.request.uri,
                "logid": self.logid,
                "errno": errno,
                "errmsg": str(e),
                "args" : str(e.args),
                "trace": traceback.format_exc(),
                "ex_type":type(e)
            }
            log.fatal("internal_error", warning)
            sys.stderr.write(pprint.pformat(warning))
            
    def work(self):
        pass
    
    def _render_result(self, errno, errmsg, data=None):
        """
        write the json data to the response

        you can override this method to render your result to the client
        """
        self.set_header("Content-Type", "application/json; charset=utf-8")
        if self._finished:
            return
        self.write(tornado.escape.json_encode({
            "errno": errno,
            "errmsg": errmsg,
            "logid": self.logid,
            "data": data,
        }))

    def getParamAsInt(self, key, default=0):
        t = self.request.arguments.get(key, [])
        if len(t) == 0:
            return default
        return util.safe_cast(t[0], int, default)

    def getParamAsFloat(self, key, default=0):
        """
        parse param to float
        """
        t = self.request.arguments.get(key, [])
        if len(t) == 0:
            return default
        return util.safe_cast(t[0], float, default)
    
    def getParamAsString(self, key, default=None):
        """
        get param string
        """
        t = self.request.arguments.get(key, [])
        if len(t) == 0:
            return default
        return str(t[0]).strip()
    
    def getAttr(self, name):
        """
        get Attribute
        """
        if hasattr(self, name):
            return getattr(self, name)
        return None
    
    def checkParamAsString(self, key):
        p = self.getParamAsString(key, None)
        if p is None or len(p) == 0:
            errmsg = "error_param:'{}'".format(key)
            log.warning(errmsg)
            raise error.BaseError(errno=error.ERRNO_PARAM, errmsg=errmsg)
        return p
    #
    post = get
    head = get
    delete = get
    patch = get
    put = get
    options = get
        