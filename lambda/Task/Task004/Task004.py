# -*- coding:utf-8 -*-
import os
import boto3
import json
from common.datetime_util import DateTimeUtil

from common.dao import Dao
from operator import itemgetter

def task004_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(DateTimeUtil.str_now())

    lambda_cnt = 6

    dao = Dao()
    brands = dao.table('stock_brands').full_scan()
    ccode_list = [r['ccode'] for r in brands]
    ccode_list.append('998407')  # 日経平均
    ccode_list.append('USDJPY')  # ドル円

    q = len(ccode_list) // lambda_cnt
    chank_ccode = [ccode_list[i:i + q] for i in range(0, len(ccode_list), q)]

    if len(chank_ccode) > lambda_cnt:
        mod_chank = chank_ccode.pop(len(chank_ccode) - 1)
        chank_ccode[-1].extend(mod_chank)

    for i, ccodes in enumerate(chank_ccode, start=1):
        func_name = 'arn:aws:lambda:ap-northeast-1:007575903924:function:Price{0:03d}'.format(i)
        cds = '_'.join(ccodes)
        print(func_name)
        boto3.client("lambda").invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps({"cds": cds})
        )


    return 0

if __name__ == '__main__':
    task004_handler({}, {})
