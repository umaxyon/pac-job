# -*- coding:utf-8 -*-
import os
from common.dao import Dao
from common.twitter_inspector import TwitterInspector
from common.key.twitter_key import Kabpackab
from common.mecab_parser import MeCabParser
from common.mecab_logic import MeCabLogic
from common.datetime_util import DateTimeUtil
from common.util import Util
from common.log import Log


def task_exp_handler(event, context):
    os.environ["PRODUCTION_DAO"] = "True"  # TODO 本番向けテスト用
    print(DateTimeUtil.str_now())

    dao = Dao()
    dao_edi = dao.table('stock_edinet')
    edis = dao_edi.full_scan()
    print('len(edi) = {}'.format(len(edis)))
    for edi in edis:
        if 'holders' not in edi:
            edi['holders'] = 'non'
        if 'holder_rate' not in edi:
            edi['holder_rate'] = 'non'
        if 'outstanding_share' not in edi:
            edi['outstanding_share'] = 'non'

        cd = edi['ccode']
        nm = edi['name']
        hr = edi['holder_rate']
        ot = edi['outstanding_share']
        tot = get(hr, 'tot')

        tabs = '{}\t' * 6
        print(tabs.format(cd, nm, get(hr, 'unit'), get(tot, 1), get(ot, 'hutuu_hakkou'), get(ot, 'unit')))

    return ''

def get(ob, key):
    if ob == 'non':
        return 'non'
    if type(key) == str:
        if key not in ob:
            return 'nonkey'
    elif type(key) == int:
        if len(ob) < key:
            return 'nonkey'
    return ob[key]


if __name__ == '__main__':
    task_exp_handler({}, {})
