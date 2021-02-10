import os
from common.migrate.old_dao import DB
from common.migrate.old_dao import OldDao
from common.dao import Dao

def tbl_report():
    # os.environ["PRODUCTION_DAO"] = "True"
    mongo = OldDao(DB())
    dynamo = Dao()

    cur_friends = mongo.table("twitter_friends").find({},{'_id': 0, 'tweet_summary': 0})
    friends = list(cur_friends)

    dynamo.table("twitter_friends").delete_all()
    dynamo.table("twitter_friends").insert(friends)


if __name__ == '__main__':
    tbl_report()