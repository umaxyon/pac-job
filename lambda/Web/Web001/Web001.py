# -*- coding:utf-8 -*-
import os
import random
from common.pacjson import JSON
from common.datetime_util import DateTimeUtil
from common.dao import Dao

def response(body):
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache"
        },
        "body": JSON.dumps(body)
    }

def get_available_table(dao):
    cond = dao.table("condition").find_by_key('Task003_list_pages_update')
    tbl, uptime = '', cond['update_time'] if cond else 'err'
    if not cond:
        tbl = 'err'
    if cond['val'] == '1,0':
        tbl = 'stock_report_list_pages_1'
    elif cond['val'] == '0,1':
        tbl = 'stock_report_list_pages_2'
    else:
        tbl = 'stock_report_list_pages_{}'.format(random.randint(1,2))

    return tbl, uptime

def web001_handler(event, context):
    print(DateTimeUtil.str_now())
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    dao = Dao()
    page = event['p'] if 'p' in event else "1"
    print('page = {}'.format(page))

    tbl, uptime = get_available_table(dao)
    if tbl == 'err':
        return response({"v": "1", "pages": [], "repos": [], "upt": "err"})

    repo_list_page = dao.table(tbl).find_by_key(page)
    ccodes = repo_list_page["ccodes"].split(',')
    repo_list_page["ccodes"] = ccodes
    repos = dao.table("stock_report").find_batch(ccodes)
    for repo in repos:
        repo['tweets'] = str(len(repo['tweets']))

    return response({"v": "1", "pages": repo_list_page, "repos": repos, "upt": uptime })

if __name__ == '__main__':
    web001_handler({}, {})