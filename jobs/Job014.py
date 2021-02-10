# -*- coding:utf-8 -*-
from datetime import timedelta
from common.dao import Dao
from common.log import tracelog
from common.datetime_util import DateTimeUtil


class Job014(object):
    """
    [job014]twitter_friends_sumの期限切れ削除
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('twitter_friends_sum取得')
        tbl_tfs = dao.table('twitter_friends_sum')
        tfs_list = tbl_tfs.full_scan()

        d = DateTimeUtil.now() - timedelta(days=15)
        two_week_ago = d.strftime('%Y/%m/%d_00:00:00')

        h1('集計開始')
        for tfs in tfs_list:
            for cd in tfs['rank']:
                ds = tfs['rank'][cd]['ds']
                n_ds = []
                for r in ds:
                    if r['d'] >= two_week_ago:
                        n_ds.append(r)
                if n_ds:
                    tfs['rank'][cd]['ds'] = n_ds
                else:
                    print("tfs['rank'][cd] = 空. cd={}".format(cd))
                    del tfs['rank'][cd]
            if tfs['rank']:
                tbl_tfs.put_item(Item=tfs)
            else:
                print('空。 uid={}'.format(tfs['uid']))
                tbl_tfs.delete_item_silent({"S": tfs['uid']})
        h1('終了')