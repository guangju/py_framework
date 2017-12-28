from util.log import log
import json

def task_handler(msg):
    log.debug("task_handler")
    msg = json.loads(msg)
    return
