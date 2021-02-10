# -*- coding:utf-8 -*-
from common.dao import Dao
from common.yahoo_stock import YahooStock
from common.log import tracelog
from common.datetime_util import DateTimeUtil

class Job015(object):
    """
    [job015]Yahooファイナンスから週次値上がり値下がり率ランキングを取得する。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        ys = YahooStock()
        h1('値上がり率取得')
        rise_list = ys.get_rise_fall_price_rate(mode='rise', period='w', today=False)
        h1('値下がり率取得')
        fall_list = ys.get_rise_fall_price_rate(mode='fall', period='w', today=False)
        h1('stock_brands_high_lowコレクションに追加')
        rise_list.extend(fall_list)
        if rise_list:
            for i in range(len(rise_list)):
                dat = rise_list[i]
                dat['date'] = DateTimeUtil.strf_ymdhms(dat['date'])
            dao.table('stock_brands_rise_fall').insert(rise_list)