# -*- coding:utf-8 -*-
import os
import json
from common.datetime_util import DateTimeUtil
from common.price_logic import PriceLogic


def price001_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(DateTimeUtil.str_now())

    logic = PriceLogic()
    logic.now_price_update(event["cds"].split('_'))
    logic.notify_now_price_update_to_condition()

    return ''

if __name__ == '__main__':
    with open('Price001_event.json', 'r') as f:
        j = json.load(f)
        price001_handler(j, None)

