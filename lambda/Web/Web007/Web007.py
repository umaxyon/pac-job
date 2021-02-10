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

def web007_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    cd = event['cd']
    print('cd = {}'.format(cd))

    edi_dat = dao.table("stock_edinet").find_by_key(cd)
    r = {}
    if edi_dat:
        r = {
            'ho': edi_dat['holders'] if 'holders' in edi_dat else {},
            'ra': edi_dat['holder_rate'] if 'holder_rate' in edi_dat else {},
            'os': edi_dat['outstanding_share'] if 'outstanding_share' in edi_dat else {},
        }


    return response({ "v": "1", "edi": r })

if __name__ == '__main__':
    print(web007_handler({'cd': '6090'}, {}))
