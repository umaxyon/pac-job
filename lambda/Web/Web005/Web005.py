# -*- coding:utf-8 -*-
import os
from common.pacjson import JSON
from common.datetime_util import DateTimeUtil
from common.dao import Dao
from common.price_logic import PriceLogic

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

def web005_handler(event, context):
    """ 現在価格API """
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()
    logic = PriceLogic()
    uptime = logic.get_now_price_update_time()

    cds = event['cds']
    print('cds = {}'.format(cds))
    cd_list = cds.split('_')

    prices = dao.table("stock_price_now").find_batch(cd_list)

    return response({ "v": "1", "prices": prices, "now_price_uptime": uptime })

if __name__ == '__main__':
    print(web005_handler({'cds': 'USDJPY_3936_8267'}, {}))
