# coding:utf-8
import os
from optparse import OptionParser

from common.log import tracelog
from common.dao import Dao
from common.log import Log

import jobs


@tracelog
def main():
    os.environ['MECAB_CHARSET'] = 'utf-8'
    op = OptionParser()
    op.add_option('-j', '--job', choices=jobs.__all__, dest='job', help='実行ジョブを指定します。(-tと併用不可')
    op.add_option('-p', '--product', action='store_true', default=False, dest='product', help='DBを本番に向ける場合に指定します。')
    opt, args = op.parse_args()

    exec_all = None
    exec_name = None
    if opt.job is not None:
        exec_all = __import__("jobs", fromlist=jobs.__all__)
        exec_name = opt.job
    else:
        raise ValueError('please set [-j].')

    exec_mod = getattr(exec_all, exec_name)
    exec_cls = getattr(exec_mod, exec_name)
    h1 = Log.headline_creator(exec_cls)
    if opt.product:
        Log.warn('**** [本番DB向き] ****')
        os.environ["PRODUCTION_DAO"] = "True"
    dao = Dao()
    exec_instance = exec_cls(dao)
    Log.info('{} start!'.format(exec_name))

    exec_instance.run(dao, h1)


if __name__ == '__main__':
    main()
