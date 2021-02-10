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

def web010_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    uid = event['uid']
    print('uid = {}'.format(uid))

    tfs = dao.table('twitter_friends_sum').find_by_key(uid)
    ranks = sorted(tfs['rank'].items(), key=lambda r: len(r[1]['ds']), reverse=True)
    ranks = ranks[:min(20, len(ranks))]
    ret = []
    for r in ranks:
        rank = {
            'cd': r[0],
            'nm': r[1]['nm'],
            'ct': len(r[1]['ds']),
            'd' : r[1]['ds'][0]['d']
        }
        ret.append(rank)

    tweet = {}
    if 'tid' in tfs:
        tweet = dao.table('tweet').find_by_key(tfs['tid'])
        del tweet['id_str']
        del tweet['user_id']
        del tweet['user_name']
        del tweet['user_screen_name']

    cond = dao.table('condition').find_by_key('Task002_tweet_report_update')
    ob = {
        'd': cond['update_time'],
        't': tweet,
        'r': ret
    }
    return response({ "v": "1", "tfs": ob })

if __name__ == '__main__':
    print(web010_handler({'uid': '811734992426450945'}, {}))
