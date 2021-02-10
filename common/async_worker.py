# coding:utf-8
import asyncio
import concurrent.futures
from itertools import chain
from multiprocessing import freeze_support

from common.log import Log
from common.log import tracelog


class AsyncWorkerException(Exception):
    def __init__(self, target):
        self.target = target


class AsyncWorker(object):
    def __init__(self, task, callback, max_workers=10):
        asyncio.set_event_loop(asyncio.SelectorEventLoop())
        freeze_support()
        self.task = task
        self.callback = callback
        self.max_workers = max_workers
        self.return_lst = []
        self.err_lst = []

    async def _async_process(self, loop, targets):
        def wrap_callback(target):
            def _cb(feature):
                task_result = feature.result()
                if type(task_result) == AsyncWorkerException:
                    self.err_lst.append(task_result.target)
                    task_result = []

                ret = self.callback(task_result, target)
                if ret:
                    if type(ret) == AsyncWorkerException:
                        self.err_lst.append(ret.target)
                    else:
                        self.return_lst.append(ret)

            return _cb

        executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers)
        for t in targets:
            Log.info('task start. target={}'.format(t))
            future = loop.run_in_executor(executor, self.task, t)
            rt = future.add_done_callback(wrap_callback(t))
            if rt:
                self.return_lst.append(rt)

        executor.shutdown(wait=True)

    @tracelog
    def go(self, targets):
        self.err_lst = []
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait([
            self._async_process(loop, targets)
        ]))
        loop.close()

        result = None
        if self.return_lst:
            if isinstance(self.return_lst[0], list):
                result = list(chain.from_iterable(self.return_lst))
            else:
                result = self.return_lst
        return result, self.err_lst
