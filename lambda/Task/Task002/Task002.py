# -*- coding:utf-8 -*-
import os
import boto3
import json
from common.mecab_parser import MeCabParser
from common.datetime_util import DateTimeUtil
from common.dao import Dao
from common.util import Util
from common.log import Log


def task002_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True" # TODO 本番向けテスト用

    print(DateTimeUtil.str_now(), 'Task002', event['ids'])

    dao = Dao()
    mecab_parser = MeCabParser()
    tweet_ids = event['ids']
    tbl_repo = dao.table("stock_report")
    tbl_tweet = dao.table('tweet')
    tbl_brands = dao.table('stock_brands')
    tbl_price_now = dao.table('stock_price_now')
    tbl_f_sum = dao.table('twitter_friends_sum')
    is_stock_find = False

    for id_str in tweet_ids:
        t = tbl_tweet.find_by_key(id_str)
        if t is None:
            continue

        t_sum = {}
        tw_txt = t["text"]
        if "retweet_text" in t:
            tw_txt = t["retweet_text"]

        # ツイートのクリーニング
        clean_twtxt = Util.mask_twitter_name(Util.all_normalize(tw_txt))

        # つぶやきを単語分割
        mecab_dic = mecab_parser.parse(clean_twtxt)
        # つぶやき内から銘柄取得
        stocks_in_tweet, words = find_stock_code_in(mecab_dic, dao, tbl_brands)
        if not is_stock_find:
            is_stock_find = len(stocks_in_tweet) > 0

        Log.info('len(stocks_in_tweet) = {}, is_stock_find = {}'.format(len(stocks_in_tweet), is_stock_find))

        for b in stocks_in_tweet.values():
            tweet_in_ccode = b["ccode"]
            Log.info('株 みっけ!! : {} {} user={}', tweet_in_ccode, b["nm"], t["user_name"])
            Log.info('tweet={}', clean_twtxt)

            stock_repo = tbl_repo.find_by_key(tweet_in_ccode)
            price = get_price(tweet_in_ccode, tbl_price_now)
            if "prices" not in t:
                t["prices"] = {}

            if tweet_in_ccode in t["prices"]:
                t["prices"][tweet_in_ccode].append(price)
            else:
                t["prices"][tweet_in_ccode] = [price]

            if not t_sum:
                t_sum = get_tweet_summary(t, tweet_in_ccode, tbl_f_sum)

            if tweet_in_ccode not in t_sum['rank']:
                t_sum['rank'][tweet_in_ccode] = {'ds': []}

            t_sum['rank'][tweet_in_ccode]['nm'] = b['nm']
            t_sum['rank'][tweet_in_ccode]['ds'].insert(0, {'d': t["created_at"], 't': t['id_str']})

            if stock_repo:
                tweets = stock_repo["tweets"]
                # 価格をpriceに付けてしまったがTweetに付けるように変更したため、価格についたpriceがあれば削除する
                if "prices" in stock_repo:
                    del stock_repo["prices"]

                if len([tw for tw in tweets if tw == t['id_str']]) == 0:
                    Log.debug('カウントアップ!! : {} {}, user={}', b["ccode"], b["nm"], t["user_name"])
                    tweets.append(t['id_str'])
                    stock_repo["tweets"] = tweets
                    stock_repo["ago_uptime"] = stock_repo["last_updated_at"]
                    stock_repo["last_updated_at"] = t["created_at"]
                    stock_repo["last_update_user"] = t["user_id"]
                    stock_repo["last_up_uname"] = t["user_name"]
                    retouch_stock_name(stock_repo, tbl_brands)
                    tbl_repo.put_item(Item=stock_repo)

            else:
                Log.debug('初登場!! : {} {}, user={}', b["ccode"], b["nm"], t["user_name"])
                data = {
                    'ccode': b["ccode"], 'name': b["nm"], 'create_user': t["user_id"],
                    'created_at': t["created_at"], 'last_updated_at': t["created_at"],
                    'last_update_user': t["user_id"], 'last_up_uname': t["user_name"],
                    'tweets': [t["id_str"]], "prices": [price]
                }
                retouch_stock_name(data, tbl_brands)
                tbl_repo.put_item(Item=data)

        if words:
            t['ws'] = words
            tbl_tweet.put_item(Item=t)

        if t_sum:
            tbl_f_sum.put_item(Item=t_sum)

    if is_stock_find:
        # Task003呼び出し
        Log.info('Task003 呼び出し')
        boto3.client("lambda").invoke(
            FunctionName="arn:aws:lambda:ap-northeast-1:007575903924:function:Task003",
            InvocationType="Event",
            Payload=json.dumps({})
        )

        dao.table("condition").update_item(
            Key={"key": "Task002_tweet_report_update"},
            ExpressionAttributeValues={
                ':val': '-',
                ':dt': DateTimeUtil.str_now()
            },
            UpdateExpression="set val = :val, update_time = :dt"
        )

    return 0


def get_tweet_summary(t, tweet_in_ccode, tbl_f_sum):
    t_user = tbl_f_sum.find_by_key(t['user_id'])
    if t_user:
        t_sum = t_user
    else:
        t_sum = {
            'uid': t['user_id'],
            'rank': {}
        }
    t_sum['unm'] = t['user_name']
    t_sum['tid'] = t['id_str']
    t_sum['d'] = t['created_at']

    return t_sum


def get_price(ccode, tbl_price_now):
    price = tbl_price_now.find_by_key(ccode)
    if price:
        del price["cd"]
        del price["d"]
    else:
        price = "-"
    return price

def retouch_stock_name(repo, tbl_brands):
    if repo['ccode'] == repo['name']:
        """stock_identifyからの当て込みの都合で名前がccodeになって
           しまったレポートについて、brandsに問い合わせて修正する"""
        brand = tbl_brands.find_by_key(repo['ccode'])
        if brand:
            repo['name'] = brand['name']


def find_stock_code_in(mecab_dic, dao, tbl_brands):
    stocks_in_tweet, words = {}, []
    for word, meta in mecab_dic.items():
        Log.debug('「{}」 : {}', word, meta)
        if not meta.get('add1') == '株':
            continue

        b = find_brand_by_identify(word, dao, tbl_brands)
        if not b:
            continue
        # コードをキーにして、１ツイート内の同一ヒット単語をdistinct
        stocks_in_tweet[b["ccode"]] = b
        words.append({word: b["ccode"]})
    return stocks_in_tweet, words

def find_brand_by_identify(word, dao, tbl_brands):
    """stock_identifyにつぶやきwordを当てて検索"""
    brands = dao.table('stock_identify').find_query({"nm": word})

    if not brands:
        Log.error('wordがstock_identifyに無い。mecab辞書とidentifyが不一致')
        return False

    if len(brands) > 1:
        brands_y = [b for b in brands if b['main'] == 'y']
        if len(brands_y) > 1:
            Log.error('mainで同じnmが複数登録されている: brands_y={}'.format(brands_y))
            return False
        elif len(brands_y) == 1:
            return brands[0]
        else:
            # ログだけ出して、複数nの１件目を使うことにする
            Log.warn('main=y がいない。nが複数。 brands={}'.format(brands))
    elif len(brands) == 1:
        b = brands[0]
        if b['nm'] == b['ccode'] and b['main'] == 'n':
            brand = tbl_brands.find_by_key(b['ccode'])
            b['nm'] = brand['name']

    return brands[0]

if __name__ == '__main__':
    with open('Task002_event.json', 'r') as f:
        j = json.load(f)
        task002_handler(j, None)