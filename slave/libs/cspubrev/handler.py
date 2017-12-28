# -*- coding=utf-8 -*-
import traceback
import cPickle
import base64
import json
import time
from tomorrow import threads
from libs import log, util
from libs.spider.pipe import Message
from libs import statistics
try:
    from libs.common import mcpack
except:
    traceback.print_exc()


stat = statistics.Statistics()

def tryMcpackLoad(receive_buf, charset):
    try:
        result_info = mcpack.loads(receive_buf, use_unicode=True, charset=charset)
        return result_info
    except Exception as e:
        log.warning("tryMcpackLoad", e)
        return False


def handle_spider_result(packet_queue, stat):
    """
    接收spider的抓取结果
    :param packet_queue: nshead接收到的数据存储在packet_queue中
    :return:
    """
    while True:
        try:
            receive_buf = packet_queue.get()
            try:
                result_info = tryMcpackLoad(receive_buf, "gb18030")
                if result_info is False:
                    result_info = tryMcpackLoad(receive_buf, "utf8")
                if result_info is False:
                    log.fatal("mcpack_loads_error")
                #result_info = mcpack.loads(receive_buf, use_unicode=True, charset="gb18030")  #TODO 是否需要更改编码
                #result_info = mcpack.loads(receive_buf)  #TODO 是否需要更改编码
                #print(result_info)
                #print(result_info.keys())
                work(result_info, stat)
            except Exception as e:
                print traceback.format_exc()
                log.fatal("handle_spider_result_err", e)
                result_info = mcpack.loads(receive_buf, use_unicode=True, charset="utf8")  #TODO 是否需要更改编码
                print(result_info)
                continue
        except:
            print traceback.format_exc()
            continue

def logDelayTime(resp):
    try:
        pipe = resp['trespassing_field']['pipe']
        delay = time.time() - resp['trespassing_field']['_log_id']
        prefix = util.current_hour()
        stat.incrRedis("cspub:delay:{}:{}".format(prefix, pipe), delay/1000.0, expire=86400)
        stat.incrRedis("cspub:callback:{}:{}".format(prefix, pipe), 1, expire=86400)
        log.notice("logDelayTime pipe: {}, delay: {}".format(pipe, delay))
    except:
        pass

def incrPipeSaverStatus(pipeName, status):
    prefix = util.current_hour()
    stat.incrRedis("saver:status-v2:{}:{}:{}".format(prefix, pipeName, status), 1, 86400)

def checkResp(resp):
    try:
        if resp['content_length'] == 0:
            if resp['result'] == 501:
                log.debug("content_length is 0, result is 501")
                retry(resp)

            return False

        else:
            return True

    except Exception as e:
       log.fatal("checkResp err: error={}, resp={}".format(e, resp))

def retry(resp):
    pipe = stat.getPipeByName(resp['trespassing_field']['pipe'])
    urlPacker = cPickle.loads(base64.b64decode(resp['trespassing_field']['urlPack']))
    if pipe is not None:
        try:
            msg = urlPacker.msg
            msg.retry = msg.retry + 1
            if msg.retry > 5:
                log.debug("retry num > 5, push to trash")
                pipe.pushToTrashList(base64.b64encode(cPickle.dumps(msg)))
                incrPipeSaverStatus(pipe.name, "error")
            else:
                log.debug("push to retry list {}".format(msg.retry))
                pipe.pushToRetryList(base64.b64encode(cPickle.dumps(msg)))

        except Exception as e:
            log.fatal("unexcept_error_on_csrev_work", e)

@threads(10)
def work(resp, stat):
    pipe = None
    root = None
    try:
        logDelayTime(resp)
        if checkResp(resp) == True:
            pipe = stat.getPipeByName(resp['trespassing_field']['pipe'])
            log.notice("got result of pipe: {}, result: {}".format(pipe.name, resp['result']))
            urlPacker = cPickle.loads(base64.b64decode(resp['trespassing_field']['urlPack']))
            root = json.loads(resp['html_body'])
            saveResult = pipe.saver.start(root, urlPacker)
            if not saveResult:
                raise RuntimeError("saver_error: pipe={}, resp={}".format(pipe.name, resp))
            incrPipeSaverStatus(pipe.name, "ok")
    except Exception as e:
        traceback.print_exc()
        log.fatal("handle_spider_result_worker_err: error={}, resp={}".format(e, resp))
        if pipe is not None:
            try:
                msg = urlPacker.msg
                msg.retry = msg.retry + 1
                if msg.retry > 5:
                    log.debug("retry num > 5, push to trash")
                    pipe.pushToTrashList(base64.b64encode(cPickle.dumps(msg)))
                    incrPipeSaverStatus(pipe.name, "error")
                else:
                    log.debug("push to retry list {}".format(msg.retry))
                    pipe.pushToRetryList(base64.b64encode(cPickle.dumps(msg)))

            except Exception as e:
                log.fatal("unexcept_error_on_csrev_work", e)
