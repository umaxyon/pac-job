# -*- coding:utf-8 -*-
import boto3
from datetime import timedelta
from common.dao import Dao
from common.datetime_util import DateTimeUtil
from common.log import tracelog
from common.async_worker import AsyncWorker
from common.async_worker import AsyncWorkerException
from common.yahoo_stock import YahooStock
from common.price_logic import PriceLogic

class Job010(object):
    """
    [job010]stock_price_historyに価格情報を作成する
    [Queue001]
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):

        h1('銘柄一覧取得')
        stock_brands = dao.table('stock_brands').full_scan()

        h1('銘柄ごとに時系列データを取得してDB登録(並列処理)')
        ys = YahooStock()

        def j010_historical(param):
            last_plus_one = param[1] + timedelta(days=1)
            str_last_plus_one_date = last_plus_one.strftime('%Y-%m-%d')

            prices = ys.get_historical_prices(ccode=param[0], str_start_date=str_last_plus_one_date)
            if not prices:
                return AsyncWorkerException(param[0])
            return prices

        # @tracelog
        def c010_historical(results, param):
            if results:
                price_list = PriceLogic().convert_history_yahoo_to_pacpac(results, param[0])
                dao.table('stock_price_history').insert(price_list)

        ccode_list = [r['ccode'] for r in stock_brands]
        ccode_list.append('998407') # 日経平均
        ccode_list.append('USDJPY') # ドル円

        param_list = []
        db_history = dao.table('stock_price_history')
        for ccode in ccode_list:
            history = db_history.get_query_sorted_max({"cd": ccode}, 1)
            if history and history[0]:
                str_last_day = history[0]['d']
                last_day = DateTimeUtil.str_to_date(str_last_day)
                param_list.append((ccode, last_day))

        result = AsyncWorker(j010_historical, c010_historical, 5).go(param_list)
        h1('終了')
        err_list = result[1]
        if err_list:
            print('[失敗銘柄] len(err) = {}, err_targets = {}'.format(len(err_list), err_list))

        # TODO stock_price_historyを月末にファイルバック（または別DB移行)して削除したい
