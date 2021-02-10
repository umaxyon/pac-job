# -*- coding: utf-8 -*-
import os
import sys
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb import table
import inspect
from common.conf.ddl import DDL
from common.const import Const
from common.util import Util
from more_itertools import chunked

class DaoException(object):
    def __init__(self, msg):
        self.msg = msg


class WrapTable(object):
    def __init__(self, tbl: table, db, client, table_name):
        self.db = db
        self.client = client
        self.table_name = table_name
        self.tbl = tbl
        self.table_exists = self.register_method()

    def register_method(self):
        try:
            for m in inspect.getmembers(self.tbl, inspect.ismethod):
                setattr(self, m[0], self._wrap(m[1]))
            return True
        except:
            e = sys.exc_info()
            if e[0].__name__ == 'ResourceNotFoundException':
                return False
            raise e

    def _wrap(self, method):
        def _m(*args, **kwargs):
            if not self.table_exists:
                raise DaoException('{} is not exists.'.format(self.table_name))
            ret = method(*args, **kwargs)
            return WrapTable._scribe_item(ret)
        return _m

    @staticmethod
    def _scribe_item(obj):
        if type(obj) == dict:
            if 'Item' in obj:
                return obj['Item']
            if 'Items' in obj:
                items = []
                for row in obj['Items']:
                    items.append(row['Item'] if type(row) == dict and 'Item' in row else row)
                return items
        return obj

    @staticmethod
    def _scribe_query_result(ret):
        def _recurcive(ob):
            if isinstance(ob, dict):
                dic = {}
                for dr in ob.items():
                    if dr[0] in ['S', 'N']:
                        return dr[1]
                    elif dr[0] in ['L', 'M']:
                        return _recurcive(dr[1])
                    elif 'NULL' != dr[0] and 'NULL' != dr[1]:
                        dic[dr[0]] = _recurcive(dr[1])
                return dic
            if isinstance(ob, list):
                lis = []
                for o in ob:
                    if 'NULL' != o:
                        lis.append(_recurcive(o))
                return lis

        try:
            return _recurcive(ret)
        except KeyError:
            print('_scribe_query_result err: {}'.format(ret))

    def delete_all(self):
        if self.table_exists:
            self.delete_table()

        self.create_table()

    def delete_table(self):
        self.tbl.delete()
        waiter = self.client.get_waiter('table_not_exists')
        waiter.wait(TableName=self.table_name)
        self.table_exists = False
        print("table [{}] was delete.".format(self.table_name))

    def create_table(self):
        ddl = DDL[self.table_name]
        self.db.create_table(**ddl)
        waiter = self.client.get_waiter('table_exists')
        waiter.wait(TableName=self.table_name)
        self.table_exists = True
        self.tbl = self.db.Table(self.table_name)
        self.register_method()

    def insert(self, items):
        err_items = []
        with self.tbl.batch_writer() as batch:
            for item in items:
                Util.blank_to_none(item)
                Util.date_to_str(item)
                try:
                    batch.put_item(Item=item)
                except ClientError:
                    try:
                        self.tbl.put_item(Item=item)
                    except ClientError as e:
                        print(e)
                        err_items.append(item)
        print("err_items: {}".format(err_items))
        return err_items

    def delete_batch_silent(self, keys):
        ddl_attr_defs = DDL[self.table_name]['AttributeDefinitions']
        if len(ddl_attr_defs) > 1:
            raise DaoException("find_by_idはHashのみ対応")
        key_nm = ddl_attr_defs[0]['AttributeName']
        err_keys = []
        with self.tbl.batch_writer() as batch:
            for key in keys:
                try:
                    batch.delete_item(Key={key_nm: key})
                except ClientError as e:
                    try:
                        if 'The provided key element does not match the schema' not in e.args[0]:
                            self.tbl.delete_item_silent(key)
                    except ClientError as e:
                        print(e)
                        err_keys.append(key)
        print("err_keys: {}".format(err_keys))
        return err_keys

    def delete_item_silent(self, key_ob):
        try:
            self.tbl.delete_item(Key=key_ob)
        except ClientError as e:
            # 指定キーが無かった場合は無視
            if 'The provided key element does not match the schema' not in e.args[0]:
                raise e

    def find_query(self, param, asc=True):
        # 複合キーテーブルに対するキー検索かつ、パラメータも取得項目もstringのみ対応
        param_list = list(param.items())
        p = param_list[0]
        ret = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="{} = :val".format(p[0]),
            ExpressionAttributeValues={":val": {"S": p[1]}},
            ScanIndexForward=asc,
        )
        ret = WrapTable._scribe_item(ret)
        if len(param_list) == 2:
            f = param_list[1]
            ret = [row for row in ret if row[f[0]]['S'] == f[1]]
        return WrapTable._scribe_query_result(ret)

    def get_query_sorted_max(self, param, limit):
        # 複合キーテーブルに対するソートキー降順での指定件数取得
        p = list(param.items())[0]
        ret = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="{} = :val".format(p[0]),
            ExpressionAttributeValues={":val": {"S": p[1]}},
            ScanIndexForward=False,
            Limit=limit
        )
        ret = WrapTable._scribe_item(ret)
        return WrapTable._scribe_query_result(ret)

    def find_by_key(self, key_val):
        ddl_attr_defs = DDL[self.table_name]['AttributeDefinitions']
        if len(ddl_attr_defs) > 1:
            raise DaoException("find_by_idはHashのみ対応")

        key = ddl_attr_defs[0]['AttributeName']
        ret = self.tbl.get_item(Key={key: key_val})
        ret = WrapTable._scribe_item(ret)
        if key not in ret:
            print('取得失敗: {}.{}={}'.format(self.table_name, key, key_val))
            print('ret={}'.format(ret))
            return None
        return ret

    def find_batch(self, key_list):
        def run(id_lst, k, dep=3):
            ret = self.client.batch_get_item(
                RequestItems={self.table_name: {'Keys': [{key: {"S": v}} for v in id_lst]}}
            )
            ret = WrapTable._scribe_query_result(ret['Responses'][self.table_name])

            if len(id_lst) == len(ret) or dep == 0:
                return ret
            else:
                id_lst = list(set(id_lst) - set([r[k] for r in ret]))
                rr = run(id_lst, k, dep - 1)
                ret.extend(rr)
                return ret

        ddl_attr_defs = DDL[self.table_name]['AttributeDefinitions']
        if len(ddl_attr_defs) > 1:
            raise DaoException("find_batchはHashのみ対応")
        key = ddl_attr_defs[0]['AttributeName']

        keys_chank = list(chunked(key_list, 100))
        results = []
        err_keys = []

        for keys in keys_chank:
            try:
                ret_lst = run(keys, key)
                results.extend(ret_lst)
            except KeyError as e:
                print(e)
            except:
                err_keys.append(keys)
                e = sys.exc_info()
                print(e)
        return results

    def full_scan(self):
        results = []
        paginator = self.client.get_paginator('scan')
        for page in paginator.paginate(TableName=self.table_name):
            if 'Items' in page and page['Items']:
                results.extend(page['Items'])

        return WrapTable._scribe_query_result(results)

class Dao(object):
    def __init__(self):
        if "PRODUCTION_DAO" in os.environ or Const.is_production():
            session = boto3.Session(aws_access_key_id=Const.AWS_ACCESS_KEY,
                                    aws_secret_access_key=Const.AWS_SECRET_ACCESS_KEY,
                                    region_name=Const.AWS_REGION_TOKYO)
            self.db = session.resource('dynamodb')
            self.client = session.client('dynamodb')
        else:
            self.db = boto3.resource('dynamodb', endpoint_url='http://localhost:8001')
            self.client = boto3.client('dynamodb', endpoint_url='http://localhost:8001')

    def table(self, table_name):
        tbl = self.db.Table(table_name)
        return WrapTable(tbl, self.db, self.client, table_name)
