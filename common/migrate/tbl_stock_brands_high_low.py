import os
from datetime import datetime
from common.migrate.old_dao import DB
from common.migrate.old_dao import OldDao
from common.dao import Dao

def tbl_stock_brands_high_low():
    # os.environ["PRODUCTION_DAO"] = "True"
    mongo = OldDao(DB())
    dynamo = Dao()

    mongoBrands = mongo.table('stock_brands_high_low').find({
        "date": { "$gte": datetime(2018, 3, 22, 9, 0, 0)}
        }, {'_id': 0 }).sort([('date', 1)]);
    mongoBrands = list(mongoBrands)[1:]

    # dynamo.table("stock_brands_high_low").insert(mongoBrands)

    # brands = dynamo.table("stock_brands_high_low").full_scan()
    #
    # brands = [b for b in brands if b['date'] > '2018/03/20_00:00:00']
    #
    # for b in brands:
    #     print(b)




if __name__ == '__main__':
    tbl_stock_brands_high_low()