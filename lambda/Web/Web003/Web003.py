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

def web003_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()

    cd = event['cd']
    last_tweet_id = event['lt'] if 'lt' in event else None
    direction = event['dr'] if 'dr' in event else None

    print('cd = {}, last_tweet_id = {}, direction = {}'.format(cd, last_tweet_id, direction))

    repo = dao.table("stock_report").find_by_key(cd)
    if not repo:
        return response({"v": "1", "tweets": []})

    tweet_ids = repo['tweets']
    end_id = tweet_ids[0]

    tweets = dao.table("tweet").find_batch(tweet_ids)
    if tweets:
        tweets = sorted(tweets, key=lambda r: r['created_at'], reverse=True)
        tweets = tweets[:30]

    target_tweets = []
    more_flg = False
    for tw in tweets:
        if direction == 'new':
            if last_tweet_id == tw['id_str']:
                break
            target_tweets.append(tw)
        else:
            if last_tweet_id == tw['id_str']:
                more_flg = True
                continue
            elif more_flg:
                target_tweets.append(tw)

    if target_tweets:
        if target_tweets[-1]["id_str"] == end_id:
            target_tweets[-1]["e"] = 1

    return response({"v": "1", "tweets": target_tweets})

if __name__ == '__main__':
    web003_handler({'cd': '3808', 'lt': '', 'dr': 'new'}, {})
    # web003_handler({'cd': '6090', 'lt': '', 'dr': 'new'}, {})
