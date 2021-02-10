# -*- coding:utf-8 -*-
import boto3
from common.dao import Dao
from common.log import tracelog

from common.yahoo_stock import YahooStock


class Job001(object):
    """
    [job001]Yahooファイナンスから銘柄一覧を取得してDBに格納する。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('stock_brandsを全件delete')
        self._delete_stock_brands()

        h1('ブランド一覧取得')
        ys = YahooStock()
        ret = ys.get_brands()
        for r in ret:
            r['market'] = r['market'].strip()
            r['info'] = r['info'].strip()

        h1('stock_brandsコレクションに追加')
        dao.table('stock_brands').insert(ret)

        h1('Job007をキック')
        boto3.client('batch').submit_job(
            jobName='Job007',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job007_stock_brands_patch_recreate",
            jobDefinition="Job007_stock_brands_patch_recreate:1"
        )

        h1('Job011をキック')
        boto3.client('batch').submit_job(
            jobName='Job011',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job011_populate_stock_detail",
            jobDefinition="Job011_populate_stock_detail:1"
        )

        h1('終了')

    def _delete_stock_brands(self):
        self.dao.table('stock_brands').delete_all()
