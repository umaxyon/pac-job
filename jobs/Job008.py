# -*- coding:utf-8 -*-
import boto3
import pandas as pd
from pandas import DataFrame
from common.dao import Dao
from common.log import tracelog


class Job008(object):
    """
    [job008]PacPac.csvを作成する
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('s3からmecab_dic.xlsxをダウンロード')
        df_dic: DataFrame = self.load_mecab_dic_dataframe()

        h1('stock_identifyを取得')
        identifies = dao.table('stock_identify').full_scan()

        h1('データフレームにstock_identifyをマージする')
        idx = len(df_dic) + 1
        stock_row = {}
        for ident in identifies:
            nm = ident['nm']
            stock_row[idx] = [nm, None, None, 10, '名詞', '一般', '*', '*', '*', '*', '*', '*', '*', '株']
            idx += 1

        df_stock = DataFrame().from_dict(stock_row, orient='index')
        df_stock.columns = df_dic.columns.values
        df_dic = df_dic.append(df_stock)

        h1('PacPac.csvを出力する')
        df_dic.to_csv(
            './PacPac.csv',
            encoding='utf-8',
            line_terminator='\n',
            index=False,
            header=False,
            mode='w'
        )

        h1('s3にPacPac.csvをアップロード')
        s3_bucket = boto3.resource('s3').Bucket('kabupac.system')
        s3_bucket.upload_file('./PacPac.csv', 'PacPac.csv')

        h1('Job005をキック')
        boto3.client('batch').submit_job(
            jobName='Job005',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job005_mecab_lambda_build",
            jobDefinition="Job005_mecab_lambda_build:1"
        )

        h1('終了')

    def load_mecab_dic_dataframe(self):
        """mecab_dic.xlsの全シートを読みだしてデータフレーム作る"""
        s3_bucket = boto3.resource('s3').Bucket('kabupac.system')
        s3_bucket.download_file('mecab_dic.xlsx', './mecab_dic.xlsx')

        xls_dic = pd.ExcelFile('./mecab_dic.xlsx')
        df_dic = None
        for sheet in xls_dic.sheet_names:
            if df_dic is None:
                df_dic = xls_dic.parse(sheet)
            else:
                df_dic = df_dic.append(xls_dic.parse(sheet))
        return df_dic
