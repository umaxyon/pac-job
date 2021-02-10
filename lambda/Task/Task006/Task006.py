# -*- coding:utf-8 -*-
import os
import boto3
from common.dao import Dao
from common.datetime_util import DateTimeUtil
from common.pacjson import JSON

"""
[Task006]suggest用jsonのs3アップロード
"""
def task006_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(DateTimeUtil.str_now())
    dao = Dao()

    ident_list = dao.table('stock_identify').full_scan()
    data = {}
    for ident in ident_list:
        cd = ident['ccode']
        dat = {} if cd not in data else data[cd]
        if 'ns' in dat:
            dat['ns'] = dat['ns'].split('_')
            dat['ns'].append(ident['nm'])
        else:
            dat['ns'] = [ident['nm']]
        if ident['main'] == 'y':
            dat['n'] = ident['nm']
        dat['ns'] = '_'.join(dat['ns'])
        data[cd] = dat
    ret = []
    for d in data.items():
        d[1]['c'] = d[0]
        ret.append(d[1])

    s3 = boto3.resource('s3').Bucket("kabupac.com")
    s3.put_object(Key="suggest/dat.json", Body=JSON.dumps(ret))

    return ''


if __name__ == '__main__':
    task006_handler({}, {})
