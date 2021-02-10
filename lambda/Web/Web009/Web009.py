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

def web009_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    cd = event['cd']
    print('cd = {}'.format(cd))

    high_low = dao.table("stock_brands_high_low").find_query({ 'ccode': cd }, asc=False)
    r = {}
    if high_low:
        high = [r for r in high_low if r['mode'] == 'high']
        low  = [r for r in high_low if r['mode'] == 'low']
        r = { 'h': high, 'l': low }
    rise_fall = dao.table("stock_brands_rise_fall").find_query({'ccode': cd}, asc=False)
    if rise_fall:
        rise = [r for r in rise_fall if r['mode'] == 'rise']
        fall = [r for r in rise_fall if r['mode'] == 'fall']
        r['r'] = rise
        r['f'] = fall

    return response({ "v": "1", "hl": r })

if __name__ == '__main__':
    print(web009_handler({'cd': '2144'}, {}))
