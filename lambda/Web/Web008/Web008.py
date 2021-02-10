# -*- coding:utf-8 -*-
import os
from common.pacjson import JSON
from common.datetime_util import DateTimeUtil
from common.dao import Dao

def response(body):
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache"
        },
        "body": JSON.dumps(body)
    }

def web008_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    prices = dao.table("stock_price_history").find_query({'cd': event['cd']})
    if prices:
        if 'lt' in event and event['lt']:
            prices = [r for r in prices if r['d'] > event['lt']]
        else:
            s = 0 if len(prices) <= 30 else abs(len(prices) - 30)
            prices = prices[s:]
    else:
        prices = []

    return response({"v": "1", "ps": prices })

if __name__ == '__main__':
    web008_handler({'cd': '6090', 'lt': ''}, {})