# -*- coding:utf-8 -*-
from datetime import timedelta
from common.dao import Dao
from common.log import tracelog
from common.datetime_util import DateTimeUtil


class Job014(object):
    """
    [job014]twitterユーザーごとのツイート集計
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('stock_report取得')
        repos = dao.table('stock_report').full_scan()
        d = DateTimeUtil.now() - timedelta(days=15)
        two_week_ago = d.strftime('%Y/%m/%d_00:00:00')
        friend_tweet = {}
        ret_list = []
        h1('ツイート集計開始')
        for repo in repos:
            cd = repo['ccode']
            tw_ids = repo['tweets']
            tweets = dao.table("tweet").find_batch(tw_ids)
            tweets = [t for t in tweets if t['created_at'] > two_week_ago]
            tweets = sorted(tweets, key=lambda r:r['created_at'], reverse=True)

            for t in tweets:
                u_id = t['user_id']
                d = t['created_at']
                ob = {
                    'nm': repo['name'],
                    'ds': [{'d': d, 't': t['id_str']}]
                }
                if u_id in friend_tweet:
                    if cd in friend_tweet[t['user_id']]:
                        friend_tweet[u_id][cd]['ds'].append({'d': d, 't': t['id_str']})
                    else:
                        friend_tweet[u_id][cd] = ob
                else:
                    friend_tweet[t['user_id']] = {}
                    friend_tweet[t['user_id']][cd] = ob

        for k in friend_tweet:
            r = {
                'uid': k,
                'rank': friend_tweet[k]
            }
            ret_list.append(r)

        dao.table('twitter_friends_sum').insert(ret_list)
        print('hoge')