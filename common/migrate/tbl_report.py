import os
from common.migrate.old_dao import DB
from common.migrate.old_dao import OldDao
from common.dao import Dao

def tbl_report():
    # os.environ["PRODUCTION_DAO"] = "True"
    mongo = OldDao(DB())
    dynamo = Dao()

    cur_repo = mongo.table("stock_report_pre").find({},{'_id': 0}).sort([('created_at', -1)])
    repos = []
    for repo in cur_repo:
        repo['tweets'] = [t['id_str'] for t in repo['tweets']]
        repos.append(repo)

    dynamo.table("stock_report").delete_all()
    dynamo.table("stock_report").insert(repos)


if __name__ == '__main__':
    tbl_report()