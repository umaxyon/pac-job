# -*- coding:utf-8 -*-
import re
import zenhan
from datetime import datetime
from common.dao import Dao
from common.log import tracelog
from common.edinet import Edinet
from common.datetime_util import DateTimeUtil
from common.log import Log


class Job013(object):
    """
    [job013]stock_edinetテーブルを作成する
    """
    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        edinet = Edinet()
        start_day = DateTimeUtil.yesterday()
        end_day = DateTimeUtil.now()
        # start_day = datetime(2018, 5, 19)
        # end_day = datetime(2018, 5, 20)

        h1('EDINETの書類検索から指定期間の有価証券報告書の検索結果を取得する。')
        search_rows = edinet.get_report_search_results(start_day, end_day)
        search_rows.reverse()

        # search_rows = [r for r in search_rows if 'スタートトゥデイ' in r['company_name']]

        h1('検索結果から有価証券報告書を取得し、銘柄毎スクレイピング開始。')
        dao_identify = dao.table('stock_identify')
        dao_brand = dao.table('stock_brands')
        not_brands = []
        for search_row in search_rows:
            data_html = edinet.get_report_html(search_row['syorui_kanri_no'])
            shorui_mei = search_row['syorui_mei']
            Log.info('html取得完了 : {} : {}'.format(search_row['company_name'], shorui_mei))
            if '大株主' not in data_html:
                Log.warn('HTMLに大株主が無い。別資料かも。 company_name : {}'.format(search_row['company_name']))
                continue

            table_list = edinet.report_html_to_split_table_list(data_html, shorui_mei)
            Log.info('HTML -> table分割 : {}'.format(search_row['company_name']))
            dat = {}
            for t in table_list:
                if t['title'] == '発行済株式' and 'outstanding_share' not in dat:
                    dat['outstanding_share'] = edinet.outstanding_share(t, search_row)
                    Log.debug('** 発行済株式 : {}'.format(search_row['company_name']))

                if t['title'] == '所有者別状況':
                    dat['holder_rate'] = edinet.holder_rate_status(t, search_row)
                    Log.debug('** 所有者別状況 : {}'.format(search_row['company_name']))

                if t['title'] == '大株主の状況':
                    dat['holders'] = edinet.major_shareholders(t, search_row)
                    Log.debug('** 大株主の状況 : {}'.format(search_row['company_name']))

            if not dat:
                continue

            name = search_row['company_name'].replace('(株)', '').replace(' ', '')
            name = re.sub(r'^　', '', name)
            name = re.sub(r'　$', '', name)

            identify = dao_identify.find_query({'nm': name})
            if not identify:
                name_s = zenhan.z2h(name, mode=1)
                identify = dao_identify.find_query({'nm': name_s})
                if not identify:
                    name_s = name_s.replace(' ', '')
                    identify = dao_identify.find_query({'nm': name_s})

            if identify and len(identify) == 1:
                brand = dao_brand.find_by_key(identify[0]['ccode'])
                Log.info('stock_brands証券コード取得。 ccode, name : {}, {}'.format(brand['ccode'], name))
                dat['ccode'] = brand['ccode']
                dat['name'] = brand['name']
                dat['date'] = DateTimeUtil.strf_ymd_st(DateTimeUtil.date_from_japanese_era(search_row['submition_day'], short=True))

                if empty(dat, 'outstanding_share') or empty(dat,'holder_rate') or empty(dat, 'holders'):
                    edi = dao.table('stock_edinet').find_by_key(dat['ccode'])
                    if edi:
                        if empty(dat, 'outstanding_share') and not empty(edi, 'outstanding_share'):
                            dat['outstanding_share'] = edi['outstanding_share']
                        if empty(dat, 'holder_rate') and not empty(edi, 'holder_rate'):
                            dat['holder_rate'] = edi['holder_rate']
                        if empty(dat, 'holders') and not empty(edi, 'holders'):
                            dat['holders'] = edi['holders']

                h1('stock_edinetに登録')
                dao.table('stock_edinet').put_item(Item=dat)
            else:
                dao.table('err_value').put_item(Item={
                    'key': 'Job013_edi_name',
                    'd': DateTimeUtil.str_now(),
                    'val': name
                })

        h1('終了')

def empty(ob, key):
    return not ob or key not in ob or not ob[key] or ob[key] == '-'
