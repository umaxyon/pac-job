# -*- coding:utf-8 -*-
from common.dao import Dao
from common.log import tracelog

from common.yahoo_stock import YahooStock
from common.datetime_util import DateTimeUtil


class Job003(object):
    """
    [job003]Yahooファイナンスから本日のストップ高とストップ安を取得する。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        ys = YahooStock()
        h1('ストップ高取得')
        high_list = ys.get_dayly_limit(mode='high')
        h1('ストップ安取得')
        low_list = ys.get_dayly_limit(mode='low')

        h1('stock_brands_high_lowコレクションに追加')
        high_list.extend(low_list)
        if high_list:
            self.date_to_string(high_list)
            dao.table('stock_brands_high_low').insert(high_list)

    def date_to_string(self, data_list):
        for i in range(len(data_list)):
            dat = data_list[i]
            dat['date'] = DateTimeUtil.strf_ymdhms(dat['date'])
