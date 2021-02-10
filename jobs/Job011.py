# -*- coding:utf-8 -*-
import time
import random
import boto3
from more_itertools import chunked
from common.datetime_util import DateTimeUtil
from common.dao import Dao
from common.yahoo_stock import YahooStock
from common.kabutan import Kabtan
from common.log import Log

class Job011(object):
    """
    [job011]stock_brandsとstock_price_historyに詳細情報追加。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    def run(self, dao: Dao, h1):
        dao_brands = dao.table("stock_brands")
        dao_thema = self.dao.table('stock_thema_ccode')
        dao_ph = self.dao.table('stock_price_history')
        dao_repo = self.dao.table('stock_report')

        h1('銘柄一覧取得')
        brands = dao_brands.full_scan()

        h1('Yahooから詳細情報取得')
        ys = YahooStock()
        ccodes = [b['ccode'] for b in brands]
        ys_details = ys.get_stock_details_async(ccodes)

        h1('Kabtanから詳細情報取得')
        ccodes_chank = list(chunked(ccodes, 50))
        kb = Kabtan()
        kb_details = []
        for i, ccodes in enumerate(ccodes_chank):
            for j, cd in enumerate(ccodes):
                # print('kabtan: cd={}'.format(cd))
                kbs = kb.get_stock_detail(cd)
                kb_details.append(kbs)
                if j % 7 == 0:
                    time.sleep(random.randint(1,2))
            print('kabtan詳細取得中 : {}'.format(i))
            time.sleep(1)

        Log.debug('brands={}, ys_details={}, kb_details={}'.format(len(ccodes), len(ys_details), len(kb_details)))

        h1('stock_brandsに登録')
        ys_err_lst = []
        kb_err_lst = []
        brands = brands
        for i, brand in enumerate(brands):
            ccode = brand['ccode']
            ys_lst = [d for d in ys_details if ccode == d['ccode']]
            kb_lst = [d for d in kb_details if ccode == d['ccode']]
            ys_detail = []
            kb_detail = {}
            if not kb_lst:
                kb_err_lst.append(ccode)
            else:
                kb_detail = kb_lst[0]

            if not ys_lst:
                ys_err_lst.append(ccode)
                continue
            else:
                ys_detail = ys_lst[0]

            # stock_brands_detail登録
            # j: jika_sougaku, b: pbr, e: per, t: nen_taka, y: nen_yasu, k: kabusuu
            nentaka = '-' if ys_detail['nen_taka'] == '-' else str(int(ys_detail['nen_taka']))
            nenyasu = '-' if ys_detail['nen_yasu'] == '-' else str(int(ys_detail['nen_yasu']))
            market = brand['market'] if 'market' in brand and brand['market'] else '-'
            info = brand['info'] if 'info' in brand and brand['info'] else '-'

            r = {'ccode': ccode,
                 'market': market,
                 'name': brand['name'],
                 'info': info,
                 'tn': emp_to_str(ys_detail['tan']),
                 'j': emp_to_str(ys_detail['jika_sougaku']),
                 'k': emp_to_str(ys_detail['hakkou_kabusuu']),
                 'e': emp_to_str(ys_detail['per']),
                 'b': emp_to_str(ys_detail['pbr']),
                 't': nentaka,
                 'y': nenyasu,
                 'kj': emp_to_str(ys_detail['keijiban'])}
            r['url'] = kb_detail['url'] if 'url' in kb_detail and kb_detail['url'] != '' else '-'
            r['cate'] = kb_detail['cate'] if 'cate' in kb_detail and kb_detail['cate'] != '' else '-'
            dao_brands.put_item(Item=r)

            repo = dao_repo.find_by_key(ccode)
            if repo:
                repo['kj'] = r['kj']
                repo['url'] = r['url']
                dao_repo.put_item(Item=repo)

            print('登録 ccode={}, i={}'.format(ccode, i))
            d = DateTimeUtil.strf_ymd_st(DateTimeUtil.today())
            ph_list = dao_ph.find_query({ 'cd': ccode, 'd': d })
            if ph_list and len(ph_list) == 1:
                ph = ph_list[0]
                ph['e'] = emp_to_str(ys_detail['per'])
                ph['b'] = emp_to_str(ys_detail['pbr'])
                ph['t'] = nentaka
                ph['y'] = nenyasu
                ph['j'] = emp_to_str(ys_detail['jika_sougaku'])
                ph['k'] = emp_to_str(ys_detail['hakkou_kabusuu'])
                dao_ph.put_item(Item=ph)

            tm = dao_thema.find_by_key(ccode)
            nms = emp_to_str(kb_detail['thema'])
            if tm:
                tm['nms'] = nms
            else:
                tm = {
                    'ccode': ccode,
                    'nms': nms
                }
            dao_thema.put_item(Item=tm)

        h1('Job012をキック')
        boto3.client('batch').submit_job(
            jobName='Job012',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job012_high_low_to_brand_and_repo",
            jobDefinition="Job012_high_low_to_brand_and_repo:1"
        )

def emp_to_str(val):
    if not val or val == '' or val == '-':
        return '-'
    return str(val)