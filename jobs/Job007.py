# -*- coding:utf-8 -*-
import boto3
import pandas as pd
from pandas import DataFrame
from common.dao import Dao
from common.log import tracelog


class Job007(object):
    """
    [job007]stock_brands_patchテーブルを作成する
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('s3からstock_brands_patch.xlsxをダウンロード')
        s3_bucket = boto3.resource('s3').Bucket('kabupac.system')
        s3_bucket.download_file('stock_brands_patch.xlsx', './stock_brands_patch.xlsx')
        xls = pd.ExcelFile('./stock_brands_patch.xlsx')
        df_add: DataFrame = xls.parse(sheet_name='追加')
        df_dell: DataFrame = xls.parse(sheet_name='削除')

        df_add['mode'] = 'A'
        df_dell['mode'] = 'D'
        df = df_dell.append(df_add)

        h1('DB登録')
        records = df.to_dict('records')
        lst = [{"ccode": str(r['ccode']),
                "nm": r['name'],
                "mode": r['mode']} for r in records ]
        tbl = dao.table("stock_brands_patch")
        tbl.delete_all()
        tbl.insert(lst)

        h1('Job006をキック')
        boto3.client('batch').submit_job(
            jobName='Job006',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job006_stock_identify_recreate",
            jobDefinition="Job006_stock_identify_recreate:2"
        )

        h1('終了')
