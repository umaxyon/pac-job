import jsm
import re
from datetime import datetime
from jsm.exceptions import CCODENotFoundException

import requests
from bs4 import BeautifulSoup

from common.datetime_util import DateTimeUtil
from common.const import CATEGORIES
from common.util import Util
from common.async_worker import AsyncWorker
from common.async_worker import AsyncWorkerException


class YahooStock(object):
    def __init__(self):
        self.q = jsm.Quotes()

    def _task_get_brand(self, code):
        try:
            return self.q.get_brand(code)
        except:
            raise AsyncWorkerException(code)

    def _task_get_brand_callback(self, results, code):
        return Util.to_simple_dict_list(results)

    def get_brands(self):
        result = AsyncWorker(self._task_get_brand, self._task_get_brand_callback).go(CATEGORIES)
        return result[0]

    def get_price(self, ccode):
        return self.q.get_price(ccode)

    def get_historical_prices(self, ccode, str_start_date=None, str_end_date=None):
        """時系列取得"""
        start_date = Util.to_date(str_start_date) if str_start_date else None
        end_date = Util.to_date(str_end_date) if str_end_date else None
        try:
            if ccode == 'USDJPY':
                return self.get_historical_prices_pac(ccode, start_date, end_date)
            else:
                results = self.q.get_historical_prices(ccode, start_date=start_date, end_date=end_date)
                return Util.to_simple_dict_list(results)
        except CCODENotFoundException:
            pass
            # Log.warn('not found. ccode={}, start_date={}, end_date={}'.format(ccode, start_date, end_date))
        return []

    def get_historical_prices_pac(self, ccode, start_date, end_date):
        if not end_date:
            end_date = datetime.today()
        ret = []
        for page in range(1,500):
            params = {'sy': start_date.year, 'sm': start_date.month, 'sd': start_date.day,
                      'ey': end_date.year, 'em': end_date.month, 'ed': end_date.day,
                      'p': page, 'tm': 'd', 'code': ccode}
            resp = requests.get('https://info.finance.yahoo.co.jp/history/', params=params)
            bs = BeautifulSoup(resp.text, "lxml")
            tbl = bs.findAll("table", attrs={"class": "boardFin yjSt marB6"})
            trs = tbl[0].findAll("tr")[1:]
            for i in range(len(trs)):
                tds = trs[i].findAll("td")
                data = [td.text for td in tds]
                d = {
                    'date': datetime.strptime(data[0], '%Y年%m月%d日'),
                    'open': data[1], 'high': data[2],
                    'low': data[3], 'close': data[4]
                }
                ret.append(d)
            if not trs:
                break

        return ret

    def get_ranking(self, mode, start_page=1, last_page=1, period='d', today=True, func=None):
        """Yahooランキング取得"""
        ret_list = []
        p = start_page
        update_date = None
        while True:
            url = "https://info.finance.yahoo.co.jp/ranking/?kd={}&mk=1&tm={}&vl=a&p={}".format(
                mode, period, p)
            html = requests.get(url, timeout=10)
            bs = BeautifulSoup(html.text, "lxml")
            if update_date is None:
                update_date = self.get_update_date(bs)
                if today and update_date < DateTimeUtil.today():
                    return []

            trs = bs.find_all('tr', {'class': 'rankingTabledata'})
            ret = func(trs, update_date)
            ret_list.extend(ret)

            if not self.has_next_page(bs) or last_page == p:
                return ret_list
            p = p + 1

    def get_rise_fall_price_rate(self, mode='rise', period='d', today=True):
        """値上がり率/値下がり率取得"""

        def func(trs, update_date):
            ret_list = []
            for tr in trs:
                td = tr.select('td')
                fixtd = 1 if period == 'd' else 0
                ret_list.append({
                    'period': period,
                    'date': update_date,
                    'mode': 'rise' if mode == 'rise' else 'fall',
                    'rank': td[0].text,
                    'ccode': td[1].select('a')[0].text,
                    'market': td[2].text,
                    'name': td[3].text,
                    'price': td[4 + fixtd].text.replace(',', ''),
                    'per': td[5 + fixtd].select('span')[0].text,
                    'vol': td[6 + fixtd].select('span')[0].text.replace(',', '')
                })
            return ret_list

        md = '1' if mode == 'rise' else '2'
        return self.get_ranking(md, last_page=2, today=today, period=period, func=func)

    def get_dayly_limit(self, mode='high', page=1, today=True):
        """ストップ高/安取得"""
        def func(trs, update_date):
            ret_list = []
            for tr in trs:
                td = tr.select('td')
                if td[1].text == '札証':
                    continue
                ret_list.append({
                    'date': update_date,
                    'mode': 'high' if mode == 'high' else 'low',
                    'ccode': td[0].select('a')[0].text,
                    'name': td[2].text,
                    'price': td[4].text.replace(',', ''),
                    'per': td[5].select('span')[0].text,
                    'vol': td[6].select('span')[0].text.replace(',', ''),
                    'most': td[7].text.replace(',', '')
                })
            return ret_list

        md = '27' if mode == 'high' else '28'
        return self.get_ranking(md, today=today, func=func)

    def has_next_page(self, bs: BeautifulSoup):
        paging_ul = bs.find_all('ul', attrs={'class': 'ymuiPagingBottom'})
        if not paging_ul:
            return False
        paging_links = paging_ul[0].find_all('a')
        if paging_links:
            if paging_links[-1].text == '次へ':
                return True
        return False

    def get_update_date(self, bs: BeautifulSoup):
        div_update_date = bs.find_all("div", attrs={"class": "dtl yjSt"})
        str_update_date = div_update_date[0].text
        m = re.search(r'(\d+)年(\d+)月(\d+)日', str_update_date)
        update_date = '{}/{}/{}'.format(
            m.group(1).zfill(4),
            m.group(2).zfill(2),
            m.group(3).zfill(2))
        return datetime.strptime(update_date, '%Y/%m/%d')

    def get_stock_detail(self, ccode):
        url = "https://stocks.finance.yahoo.co.jp/stocks/detail/?code={}".format(ccode)
        html = requests.get(url, timeout=10)
        bs = BeautifulSoup(html.text, "lxml")
        li = bs.select('ul.subNavi li')
        dd = bs.select('div#rfindex dd strong')
        ret = {
            'ccode': ccode,
            'jika_sougaku': self.normalize(dd[0].text),
            'hakkou_kabusuu': self.normalize(dd[1].text),
            'per': self.normalize(dd[4].text),
            'pbr': self.normalize(dd[5].text),
            'tan': self.normalize(dd[9].text),
            'nen_taka': self.normalize(dd[10].text),
            'nen_yasu': self.normalize(dd[11].text),
            'keijiban': li[5].a.get('href')
        }
        return ret

    def get_stock_details_async(self, ccodes):
        def _cb(results, code):
            return results

        result, err = AsyncWorker(self.get_stock_detail, _cb).go(ccodes)
        return result

    def normalize(self, item):
        ret = '-'
        if not item or item is None:
            return ret
        ret = item.replace('(連) ', '').replace(',', '')

        try:
            if '.' in ret:
                ret = float(ret)
            else:
                ret = int(ret)
        except:
            ret = '-'
        return ret


if __name__ == '__main__':
    YahooStock().get_dayly_limit(
        mode='high',
        today=False)
