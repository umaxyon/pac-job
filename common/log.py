# coding:utf-8
import os
from os import path
import sys
import time
import traceback
from queue import Queue
from logutils.queue import QueueHandler, QueueListener

from logging import DEBUG
from logging import getLogger
from logging.config import fileConfig


from common.util import Util

conf_file = 'conf' + os.sep + 'logging_production.conf'

fileConfig(os.path.join(path.dirname(path.abspath(__file__)), conf_file))


class MultiProcessLogger(object):
    def __init__(self):
        self.q = Queue(-1)
        self.ql = QueueListener(self.q, *tuple(getLogger('PacPac').handlers))
        self.qh = QueueHandler(self.q)

    def get_logger(self):
        lg = getLogger("queue_listen")
        lg.addHandler(self.qh)
        lg.setLevel(DEBUG)
        lg.propagate = False  # 上位(root)に伝搬させない
        self.ql.start()
        return lg

    def end_log_listen(self):
        self.ql.stop()


MULTI_PROCESS_LOGGER = MultiProcessLogger()

logger = MULTI_PROCESS_LOGGER.get_logger()


class Color:
    normal = "\033[0m"
    black = "\033[30m"
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    blue = "\033[34m"
    purple = "\033[35m"
    cyan = "\033[36m"
    grey = "\033[37m"
    white = "\033[38m"
    bold = "\033[1m"
    uline = "\033[4m"
    blink = "\033[5m"
    invert = "\033[7m"


class Log(object):
    @staticmethod
    def set_logger(custom_logger):
        global logger
        logger = custom_logger

    @staticmethod
    def format(msg, *args):
        if not args:
            return msg
        return msg.format(*args)

    @staticmethod
    def info(msg, *args):
        fi = Util.get_called_func_info()
        f_msg = Log.format(msg, *args)
        f_msg = Log.format('[{}:{}] {}', fi[0], fi[1], f_msg)
        logger.info(Log.c(f_msg, Color.green))

    @staticmethod
    def debug(msg, *args):
        f_msg = Log.format(msg, *args)
        logger.debug(Log.c(f_msg, Color.grey))

    @staticmethod
    def warn(msg, *args):
        f_msg = Log.format(msg, *args)
        logger.warning(Log.c(f_msg, Color.yellow))

    @staticmethod
    def error(msg, *args):
        f_msg = Log.format(msg, *args)
        logger.error(Log.c(f_msg, Color.red))

    @staticmethod
    def critical(msg, *args):
        f_msg = Log.format(msg, *args)
        logger.critical(Log.c(f_msg, Color.red))

    @staticmethod
    def title(msg):
        sep = "#" * max(30, len(msg.encode('utf-8')))
        logger.info(Log.c(sep, Color.cyan))
        logger.info(Log.c(msg, Color.cyan))

    @staticmethod
    def print_stacktrace():
        e = sys.exc_info()
        Log.error('[{}] {}\n{}'.format(
            e[0].__name__, e[1], ''.join(traceback.format_exception(*e))))

    @staticmethod
    def headline_creator(obj):
        cnt = [1]
        name = obj.__name__

        def _wrap(msg):
            Log.title('[{}] {}. {}'.format(name, cnt[0], msg))
            cnt[0] += 1

        return _wrap

    @staticmethod
    def c(msg, color):
        return str(msg)
        # return color + str(msg) + Color.normal


def tracelog(func):
    """ メソッド開始終了トレースログ用ジェネレータ """

    def _wrap(*args, **kargs):
        call_func_name = func.__qualname__
        t = time.time()
        logger.debug(Log.c('({}) tracelog start.'.format(call_func_name), Color.grey))
        result = func(*args, **kargs)
        logger.debug(Log.c('({}) tracelog end. {}[s]'.format(call_func_name, (time.time() - t)), Color.grey))
        return result

    return _wrap
