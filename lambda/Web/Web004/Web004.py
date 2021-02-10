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

def web004_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    ids = event['ids']
    print('ids = {}'.format(ids))
    id_list = ids.split('_')

    friends = dao.table("twitter_friends").find_batch(id_list)

    return response({"v": "1", "friends": friends})

if __name__ == '__main__':
    print(web004_handler({'ids': '1105913665_339650190_3178567194_755954121132298240_887691814236377090_2531696492'}, {}))
