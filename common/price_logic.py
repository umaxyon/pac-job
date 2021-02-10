import re
import requests

from common.dao import Dao
from common.util import Util
from common.const import YAHOO_STOCK_DETAIL
from common.datetime_util import DateTimeUtil


class PriceLogic(object):
    def __init__(self):
        self.dao = Dao()
        self.tbl_condition = self.dao.table("condition")

    def day_key_now(self):
        return DateTimeUtil.strf_ymd_st(DateTimeUtil.now())

    def now_price_update(self, ccodes):
        db_price_now = self.dao.table('stock_price_now')
        db_price_history = self.dao.table('stock_price_history')

        str_now = self.day_key_now()
        for cd in ccodes:
            price = self.get_now_price_from_yahoo_stock_detail(cd)
            now = db_price_now.find_by_key(cd)
            if now and now['d'] == str_now:
                now['p'] = price
            else:
                history = db_price_history.get_query_sorted_max({"cd": cd}, 1)
                now = {
                    "cd": cd, "p": price, "d": str_now,
                    "lp": history[0]['c'] if history and history[0] else "-"
                }

            db_price_now.put_item(Item=now)

    def get_now_price_from_yahoo_stock_detail(self, cd):
        """Yahooファイナンスの銘柄ページのHTMLから現在株価を取得する"""
        html = requests.get(YAHOO_STOCK_DETAIL, {'code': cd}, timeout=15)
        m = re.search('<td class="stoksPrice">(.+)</td>', html.text)
        if m:
            price = m.group(1).replace(',', '')
            if cd == 'USDJPY':
                price = re.sub('0000$', '', price)
            else:
                price = re.sub('\.\d*$', '', price)
            if Util.is_digit(price):
                return price
        return "-"

    def notify_now_price_update_to_condition(self):
        self.dao.table("condition").update_item(
            Key={"key": "Task004_now_price_update"},
            ExpressionAttributeValues={
                ':val': '-',
                ':dt': DateTimeUtil.str_now()
            },
            UpdateExpression="set val = :val, update_time = :dt"
        )

    def get_now_price_update_time(self):
        cond_uptime = self.tbl_condition.find_by_key('Task004_now_price_update')
        return cond_uptime["update_time"]

    def convert_history_yahoo_to_pacpac(self, yahoo_list, ccode):
        pac_list = []
        for yh in yahoo_list:
            d = DateTimeUtil.strf_ymd_st(yh['date'])
            # d: date, o: open, l: low, h: high, c: close, v: volume
            # j: jika_sougaku, b: pbr, e: per, t: nen_taka, y: nen_yasu, k: kabusuu
            if ccode == 'USDJPY':
                pac_list.append({
                    "cd": ccode,
                    "d": d,
                    "o": re.sub('0000$', '', yh["open"]),
                    "l": re.sub('0000$', '', yh["low"]),
                    "h": re.sub('0000$', '', yh["high"]),
                    "c": re.sub('0000$', '', yh["close"]),
                    "v": "-"
                })
            else:
                pac_list.append({
                    "cd": ccode,
                    "d": d,
                    "o": str(int(yh["open"])) if yh["open"] != '-' else '-',
                    "l": str(int(yh["low"])) if yh["low"] != '-' else '-',
                    "h": str(int(yh["high"])) if yh["high"] != '-' else '-',
                    "c": str(int(yh["close"])) if yh["close"] != '-' else '-',
                    "v": str(int(yh["volume"])) if yh["volume"] != '-' else '-',
                })
        return pac_list

if __name__ == '__main__':
    import os
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(PriceLogic().get_now_price_from_yahoo_stock_detail('4308'))
