# -*- coding:utf-8 -*-
import os
from common.dao import Dao
from common.twitter_inspector import TwitterInspector
from common.key.twitter_key import Kabpackab
from common.mecab_parser import MeCabParser
from common.mecab_logic import MeCabLogic
from common.datetime_util import DateTimeUtil
from common.util import Util
from common.log import Log


def task005_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(DateTimeUtil.str_now())

    dao = Dao()
    mecab_parser = MeCabParser()
    mecab_logic = MeCabLogic()

    dao_ir = dao.table('stock_ir')

    # 前回取得した最後のtweetidを取得
    dat = dao.table('condition').get_item(Key={'key': 'Task005_last_tweet_id'})
    last_tweet_id = dat['val']

    print('last_tweet_id: ', last_tweet_id)

    tw = TwitterInspector(Kabpackab)
    timeline = tw.get_list_timeline_rotate(
        list_name='IR', count=1000, last_data_id=last_tweet_id)
    print(timeline)
    if timeline:
        data_list = []
        for t in timeline:
            tw_txt = t["text"]

            # ツイートのクリーニング
            clean_twtxt = Util.mask_twitter_name(Util.all_normalize(tw_txt))

            # つぶやきを単語分割
            mecab_dic = mecab_parser.parse(clean_twtxt)
            # つぶやき内から銘柄取得
            stocks_in_tweet, _ = mecab_logic.find_stock_code_in(mecab_dic)

            for b in stocks_in_tweet.values():
                tweet_in_ccode = b["ccode"]
                Log.info('IR みっけ!! : {} {} user={}', tweet_in_ccode, b["nm"], t["user_name"])
                data = {
                    'cd': b["ccode"], 'nm': b["nm"], 'u': t["user_id"],
                    'd': t["created_at"], 't': tw_txt, 'tid': t['id_str']
                }
                data_list.append(data)

        dao_ir.insert(data_list)

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

    return ''


if __name__ == '__main__':
    task005_handler({}, {})
