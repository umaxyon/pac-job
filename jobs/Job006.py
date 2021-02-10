# -*- coding:utf-8 -*-
import re
import boto3
import json
from common.dao import Dao
from common.log import tracelog
from common.util import Util

class Identify(object):
    def __init__(self):
        self.lst = []

    class IdentifyRow(object):
        def __init__(self, cd, ident):
            self.ident = ident
            self.ccode = cd

        def create_ident(self, name):
            name = Util.all_normalize(name)
            name = name.replace('(株)', '')
            self.add(name, main="y")
            self.add(self.ccode)
            self.cope_with_point(name)
            self.cope_with_verbose_name(name)
            self.cope_with_reduction_name(name)

        def add(self, name, main="n"):
            if len([r for r in self.ident.lst if r['nm'] == name and r['ccode'] == self.ccode]) == 0:
                self.ident.lst.append({"nm": name, "ccode": self.ccode, "main": main})

        def delete(self, name):
            lst = []
            for r in self.ident.lst:
                if r['nm'] == name and r['ccode'] == self.ccode:
                    continue
                lst.append(r)
            self.ident.lst = lst

        def cope_with_point(self, name):
            """「・」に対処する"""
            if '・' in name:
                buf = name.split('・')
                self.add(''.join(buf))
                self.add(buf[0] + '・' + buf[1])
                self.add(buf[0] + buf[1])
                if buf[0] not in ['ジャパン', 'ビジネス', 'エン']:
                    if len(buf[0]) > 2:
                        self.add(buf[0])

        def cope_with_verbose_name(self, name):
            """冗長な名前に対処する"""
            for verb in [
                r'ホールディングス$',
                r'工業$',
                r'グループ$',
                r'コーポレーション$',
                r'ジャパン']:
                if re.search(verb, name):
                    name = re.sub(verb, '', name)
                    name = re.sub(r'・$', '', name)
                    if len(name) > 2:
                        self.add(name)

        def cope_with_reduction_name(self, name):
            """短縮系に対処する"""
            if re.search(r'総合研究所$', name):
                name = re.sub(r'総合研究所$', '総研', name)
                self.add(name)

            if re.search(r'製作所$', name):
                name = re.sub(r'製作所$', '製', name)
                self.add(name)

    def ccode(self, cd):
        return Identify.IdentifyRow(cd, self)


class Job006(object):
    """
    [job006]stock_brandsからstock_identifyを作成する
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('stock_brands取得')
        brands = dao.table('stock_brands').full_scan()
        print('stock_brands scan. count={}'.format(len(brands)))

        h1('Identify作成処理')
        ident = Identify()
        for brand in brands:
            row = ident.ccode(brand['ccode'])
            row.create_ident(brand['name'])

        print('create ident. count={}'.format(len(ident.lst)))

        h1('Identifyにstock_brands_patchを適用')
        patchs = dao.table('stock_brands_patch').full_scan()
        for patch in patchs:
            mode = patch['mode']
            row = ident.ccode(patch['ccode'])
            if mode == 'A':
                row.add(patch['nm'])
            elif mode == 'D':
                row.delete(patch['nm'])

        h1('stock_identifyテーブル再作成')
        if len(ident.lst) > 0:
            tbl_identify = dao.table('stock_identify')
            tbl_identify.delete_all()
            tbl_identify.insert(ident.lst)
            print('ok')
        else:
            print('stock_ident create error. len(ident.lst): {}'.format(len(ident.lst)))

        h1('Job008をキック')
        boto3.client('batch').submit_job(
            jobName='Job008',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job008_create_pacpac_csv",
            jobDefinition="Job008_create_pacpac_csv:1"
        )

        h1('Job013をキック')
        boto3.client('batch').submit_job(
            jobName='Job013',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job013_edi_net_scraping",
            jobDefinition="Job013_edi_net_scraping:1"
        )

        h1('Task006をキック')
        boto3.client("lambda").invoke(
            FunctionName="arn:aws:lambda:ap-northeast-1:007575903924:function:Task006",
            InvocationType="Event",
            Payload=json.dumps({})
        )

        h1('終了')
