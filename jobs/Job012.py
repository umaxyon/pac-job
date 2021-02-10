from common.dao import Dao
from common.log import tracelog
from common.datetime_util import DateTimeUtil


class Job012(object):
    """
    [job012]stock_brands_high_lowをbrandsとreportに振り分ける
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        dao_brand = dao.table('stock_brands')
        dao_repo = dao.table('stock_report')
        dao_hl = dao.table('stock_brands_high_low')

        h1('stock_brands取得')
        brands = dao_brand.full_scan()
        for brand in brands:
            # stock_brandsにhigh_lowデータ追加
            cd = brand['ccode']
            high_list, low_list = self.collect_high_low_list(cd, dao_hl)
            brand['hs'] = high_list
            brand['ls'] = low_list
            dao_brand.put_item(Item=brand)

            # stock_reportに3日以内S高S安をマーク
            repo = dao_repo.find_by_key(cd)
            if repo:
                mark = repo['mk'] if 'mk' in repo else {}
                mark = self.mark_high_low('hs', high_list, mark)
                mark = self.mark_high_low('ls', low_list, mark)

                repo['mk'] = mark
                dao_repo.put_item(Item=repo)

    def mark_high_low(self, key, lst, mark):
        ymd_3dayago = DateTimeUtil.strf_ymd(DateTimeUtil.prevday_weekday(3))
        if len(lst) == 0:
            mark[key] = '-'
        else:
            d = lst[0]['d'].split('_')[0]
            mark[key] = '-' if d < ymd_3dayago else '*'
        return mark

    def collect_high_low_list(self, ccode, dao):
        hl_list = dao.find_query({'ccode': ccode}, asc=False)
        h_list, l_list = [], []
        for row in hl_list:
            r = {
                'd': row['date'],
                'p': row['most'],
                'a': row['per'],
                'v': row['vol']
            }
            if row['mode'] == 'high':
                h_list.append(r)
            elif row['mode'] == 'low':
                l_list.append(r)
        return h_list, l_list