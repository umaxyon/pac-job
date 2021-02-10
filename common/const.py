# coding:utf-8
import os
import re
from enum import Enum


class Const(object):
    AWS_ACCESS_KEY = ''
    AWS_SECRET_ACCESS_KEY = ''
    AWS_REGION_TOKYO = 'ap-northeast-1'

    @staticmethod
    def is_production():
        return not os.name == 'nt'


YAHOO_STOCK_DETAIL = "https://stocks.finance.yahoo.co.jp/stocks/detail/"


TEST_USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36')

CATEGORIES = {
    '0050': '農林・水産業',
    '1050': '鉱業',
    '2050': '建設業',
    '3050': '食料品',
    '3100': '繊維製品',
    '3150': 'パルプ・紙',
    '3200': '化学',
    '3250': '医薬品',
    '3300': '石油・石炭製品',
    '3350': 'ゴム製品',
    '3400': 'ガラス・土石製品',
    '3450': '鉄鋼',
    '3500': '非鉄金属',
    '3550': '金属製品',
    '3600': '機械',
    '3650': '電気機器',
    '3700': '輸送機器',
    '3750': '精密機器',
    '3800': 'その他製品',
    '4050': '電気・ガス業',
    '5050': '陸運業',
    '5100': '海運業',
    '5150': '空運業',
    '5200': '倉庫・運輸関連業',
    '5250': '情報・通信',
    '6050': '卸売業',
    '6100': '小売業',
    '7050': '銀行業',
    '7100': '証券業',
    '7150': '保険業',
    '7200': 'その他金融業',
    '8050': '不動産業',
    '9050': 'サービス業'
}

PTN_URL = re.compile('https?://[-_.!~*\'()a-zA-Z0-9;/?:@&=+$,%#]+')

PTN_FIGURE = [r'\\\d[\d,.]*', r'\$\d[\d,.]*']
for digit in ['兆', '億', '万', '']:
    for unit in ['円', '回', '個', '人', '点', '株', '前後', 'くらい', '位', '程', '台',
                 'ドル', '乗せ', '超え', '突破', '通過', '追加', '以下', '以上', '%', '年',
                 '']:
        if not digit and not unit:
            continue
        ptn = "[\+\-約]?\d[\d,.]*" + digit + unit
        PTN_FIGURE.append(re.compile(ptn))


class RedisDB(Enum):
    Now_stock_price = 0
    Web_row_detail = 1
    Login_pacident = 2
    Price_History = 3
    System = 4
