import os

from pymongo import MongoClient
from pymongo.collection import Collection


class DB(object):

    def __init__(self):
        self.client = MongoClient('mongodb://%s:%s@127.0.0.1:29457/dbpacpac' % ('pacPac', 'Npa98ksA'))


class OldDao(object):
    def __init__(self, db):
        self._db = db

    def table(self, col_name, dbname='dbpacpac') -> Collection:
        return self._db.client[dbname][col_name]


