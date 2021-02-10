import re

from common.dao import Dao
from common.log import tracelog

from common.key.twitter_key import Kabpackab
from common.twitter_inspector import TwitterInspector


class Job004(object):
    """
    [job004]Twitterからテーマ株を取得する。
    ### テーマ株アカウントがなくなったので中止中
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('タイムライン取得')
        tw = TwitterInspector(Kabpackab)
        timeline = tw.get_list_timeline_rotate(
            list_name='テーマ', count=2000)

        h1('テーマをスクレイピング')
        nm_codes_dict, ccode_nms_dict = {}, {}
        for t in timeline:
            line = t['text'].split('\n')
            m = re.match(r'^#(\S+)', line.pop(0))
            if m is None:
                continue
            nm = m.group(1)
            if nm not in nm_codes_dict:
                nm_codes_dict[nm] = set()

            for stock_line in line:
                ccode = re.match(r'^(\d+)', stock_line).group(1)
                nm_codes_dict[nm].add(ccode)

                if ccode not in ccode_nms_dict:
                    ccode_nms_dict[ccode] = [nm]
                else:
                    ccode_nms_dict[ccode].append(nm)

        h1('stock_thema_nmを取得してマージ')
        thema_nm_list, thema_ccode_list = [], []
        for nm, ccodes_set in nm_codes_dict.items():
            db_thema = dao.table('stock_thema_nm').get_item(Key={'nm': nm})
            if db_thema:
                db_ccode_list = db_thema['ccodes'].split(',')
                ccodes_set.update(db_ccode_list)
            thema_nm_list.append({
                'nm': nm,
                'ccodes': str(ccodes_set)[1:-1].replace('\'', '').replace(' ', '')
            })

        h1('stock_thema_ccodeを取得してマージ')
        ccode_list = ccode_nms_dict.keys()
        print(len(ccode_list), len(set(ccode_list)))
        for cd, nms_list in ccode_nms_dict.items():
            row = dao.table('stock_thema_ccode').get_item(Key={'ccode': cd})
            if row:
                db_nms = row['nms'].split(',')
                db_nms.extend(nms_list)
                row['nms'] = ','.join(list(set(db_nms)))
                thema_ccode_list.append(row)
            else:
                thema_ccode_list.append({
                    'ccode': cd,
                    'nms': ','.join(list(set(nms_list)))
                })

        h1('stock_thema_nm保存')
        for r in thema_nm_list:
            dao.table('stock_thema_nm').put_item(Item=r)

        h1('stock_thema_ccode保存')
        for r in thema_ccode_list:
            dao.table('stock_thema_ccode').put_item(Item=r)

        h1('終了')
