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

def web011_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    key = event['k']
    p = int(event['p']) - 1

    print('key = {}'.format(key))

    thema = dao.table('stock_thema_nm').find_by_key(key)
    ccodes, repos = [], []

    if thema:
        ccodes = thema['ccodes'].split(',')
        repos = dao.table('stock_report').find_batch(ccodes)
        repos = repos[p * 10:min((p + 1) * 10, len(repos))]
        for repo in repos:
            repo['tweets'] = str(len(repo['tweets']))

    return response({ "v": "1", "cds": ccodes, "repos": repos })

if __name__ == '__main__':
    print(web011_handler({'k': 'プラント', 'p': '1'}, {}))
