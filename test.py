# -*- coding: utf-8 -*-
import boto3
import pprint
from common.async_worker import AsyncWorker
from common.const import CATEGORIES
from common.dao import Dao


def put_dynamo():
    db = Dao().table('stock_brands')
    items = db.get_item(
        Key={'ccode': '6090'}
    )
    items = db.scan()
    pprint.pprint(items)
    print('stock_brandsに登録しました。')


def _task_get_brand(code):
    print('task:', code)
    return [code]


def _task_get_brand_callback(results, code):
    print('callback:', code)
    return results


def exec_async():
    result = AsyncWorker(_task_get_brand, _task_get_brand_callback).go(CATEGORIES)
    print('{}'.format(result))


if __name__ == '__main__':
    put_dynamo()
    # exec_async()
