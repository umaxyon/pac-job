# -*- coding:utf-8 -*-
import boto3
import json
from common.datetime_util import DateTimeUtil
from common.dao import Dao
from common.twitter_inspector import TwitterInspector
from common.key.twitter_key import Irebaburn

def task001_handler(event, context):
    print(DateTimeUtil.str_now())

    dao = Dao()
    # 前回取得した最後のtweetidを取得
    dat = dao.table('condition').get_item(Key={'key': 'Task001_last_tweet_id'})
    last_tweet_id = dat['val']

    print('last_tweet_id: ', last_tweet_id)

    tw = TwitterInspector(Irebaburn)
    timeline = tw.get_list_timeline_rotate(
        list_name='株', count=1000, last_data_id=last_tweet_id)
    print(timeline)
    if timeline:
        ids = [t['id_str'] for t in timeline]
        # 重複取得されたレコードがある場合、削除して入れなおすため、一旦消す。
        dao.table('tweet').delete_batch_silent(ids)
        # ids = []
        # for tweet in timeline:
        #     # 重複取得されたレコードがある場合、削除して入れなおすため、一旦消す。
        #     dao.table('tweet').delete_item_silent({'id_str': tweet['id_str']})
        #
        #     ids.append(tweet['id_str'])

        # 追加
        dao.table('tweet').insert(timeline)

        last_tweet_id = timeline[0]['id_str']  # 降順の先頭(取得した最新)
        print('last_tweet_id for update: ', last_tweet_id)

        # last_tweet_id 更新
        dao.table("condition").update_item(
            Key={"key": dat['key']},
            ExpressionAttributeValues={
                ':val': last_tweet_id,
                ':dt': DateTimeUtil.str_now()
            },
            UpdateExpression="set val = :val, update_time = :dt"
        )

        # Task002呼び出し
        boto3.client("lambda").invoke(
            FunctionName="arn:aws:lambda:ap-northeast-1:007575903924:function:Task002",
            InvocationType="Event",
            Payload=json.dumps({"ids": ids})
        )

    return ''
