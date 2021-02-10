import os
import io
import pandas as pd
from datetime import datetime
from common.migrate.old_dao import DB
from common.migrate.old_dao import OldDao
from common.dao import Dao
from common.datetime_util import DateTimeUtil
from common.datetime_util import DayOfWeek


def tbl_price():
    # os.environ["PRODUCTION_DAO"] = "True"
    mongo = OldDao(DB())
    dynamo = Dao()

    ccodes = mongo._db.client['price_history'].collection_names()
    # d: date
    # o: open
    # l: low
    # h: high
    # c: close
    # v: volume
    # j: jika_sougaku
    # b: pbr
    # e: per
    # t: nen_taka
    # y: nen_yasu
    # k: kabusuu
    col = ['d', 'o', 'l', 'h', 'c', 'v', 'j', 'b', 'e', 't', 'y', 'k']
    for ccode in ccodes:
        ccode_path = 'c:/temp/pac_price/{}'.format(ccode)
        if not os.path.exists(ccode_path):
            os.mkdir(ccode_path)

        cur_price = mongo.table(ccode, dbname='price_history').find({}, {'_id': 0}).sort([('date', 1)])
        last_path, last_month, csv_path, now_month,datas = None, 0,'', 0, []
        for i, p in enumerate(cur_price):
            date = p['date']
            str_ym = date.strftime('%Y%m')
            csv_path = '{}/{}'.format(ccode_path, str_ym)
            now_month = int(str_ym[-2:])
            if last_path and last_path != csv_path and not os.path.exists(csv_path) and now_month in [4, 7, 10, 1]:
                df = pd.DataFrame(datas, columns=col)
                df.to_csv(last_path + '.csv', encoding="utf-8", line_terminator='\n', index=False)
                datas = []


            d = DateTimeUtil.strf_ymdhms(date)
            j = p['jika_sougaku'] if 'jikasougaku' in p else ''
            b = p['pbr'] if 'pbr' in p else ''
            e = p['per'] if 'per' in p else ''
            t = p['nen_taka'] if 'nen_taka' in p else ''
            y = p['nen_yasu'] if 'nen_yasu' in p else ''
            k = p['hakkou_kabusuu'] if 'hakkou_kabusuu' in p else ''

            datas.append([
                d, p['open'], p['low'], p['high'], p['close'], p['volume'],
                j, b, e, t, y, k
            ])
            last_path = csv_path

        if now_month not in [3, 6, 9, 12]:
            now_month += (3 - now_month // 3)
        buf = csv_path[:-2]
        csv_path = buf + '{0:02d}.csv'.format(now_month)
        df = pd.DataFrame(datas, columns=col)
        df.to_csv(csv_path, encoding="utf-8", line_terminator='\n', index=False)


if __name__ == '__main__':
    tbl_price()
