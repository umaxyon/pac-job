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

def web002_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()
    cd = event['cd']
    print('cd={}'.format(cd))
    brand = dao.table("stock_brands").find_by_key(cd)
    thema = dao.table("stock_thema_ccode").find_by_key(cd)
    if thema:
        brand['thema'] = thema['nms']

    return response({"v": "1", "brand": brand })

if __name__ == '__main__':
    web002_handler({'cd': '3825'}, {})