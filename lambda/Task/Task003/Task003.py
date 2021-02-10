# -*- coding:utf-8 -*-
import os
import boto3
from operator import itemgetter

from common.dao import Dao
from common.datetime_util import DateTimeUtil


def notify_list_pages_update_to_condition(dao, val):
    dao.table("condition").update_item(
        Key={"key": "Task003_list_pages_update"},
        ExpressionAttributeValues={
            ':val': val,
            ':dt': DateTimeUtil.str_now()
        },
        UpdateExpression="set val = :val, update_time = :dt"
    )

def task003_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用

    dao = Dao()
    repos = dao.table('stock_report').full_scan()
    print('len(repos)={}'.format(len(repos)))
    repos = list({v['ccode']: v for v in repos}.values())
    print('unique. len(repos)={}'.format(len(repos)))
    repos = sorted(repos, key=itemgetter('last_updated_at'), reverse=True)[:min(len(repos),1000)]

    list_pages = [{"p": "1", "ccodes": []}]
    page_cnt = 1
    for i, repo in enumerate(repos, start=1):
        list_pages[page_cnt - 1]["ccodes"].append(repo["ccode"])

        if i < len(repos) and i % 10 == 0:
            page_cnt += 1
            list_pages.append({"p": str(page_cnt), "ccodes": []})

    for row in list_pages:
        row['ccodes'] = ','.join(row["ccodes"])

    notify_list_pages_update_to_condition(dao, '0,1')

    pages1 = dao.table('stock_report_list_pages_1')
    pages1.delete_all()
    pages1.insert(list_pages)

    notify_list_pages_update_to_condition(dao, '1,0')

    pages2 = dao.table('stock_report_list_pages_2')
    pages2.delete_all()
    pages2.insert(list_pages)

    notify_list_pages_update_to_condition(dao, '1,1')

    return 0

if __name__ == '__main__':
    task003_handler({}, {})
