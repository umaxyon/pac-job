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

def web006_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    cd = event['cd']
    last_id = event['lt'] if 'lt' in event else None
    print('cd = {}, last_id = {}'.format(cd, last_id))

    irs = dao.table("stock_ir").find_query({'cd': cd }, asc=False)
    buf_irs = []
    if last_id:
        for ir in irs:
            # stock_ir テーブルはソートキー「d」の降順。
            if ir['tid'] == last_id:
                break
            buf_irs.append(ir)
    else:
        buf_irs = irs


    return response({ "v": "1", "irs": buf_irs })

if __name__ == '__main__':
    print(web006_handler({'cd': '2370'}, {}))
