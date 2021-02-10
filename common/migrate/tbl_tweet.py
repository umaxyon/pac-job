import os
from common.migrate.old_dao import DB
from common.migrate.old_dao import OldDao
from common.dao import Dao

def tbl_tweet():
    mongo = OldDao(DB())

    os.environ["PRODUCTION_DAO"] = "True"
    dynamo = Dao()

    cur_twt = mongo.table("tweet").find({}).sort([('created_at', -1)])
    ids = [t['id_str'] for t in cur_twt]
    # ids = []
    # with open('non_exists_tweet_ids7.txt', 'r') as f:
    #     ids = f.readlines()
    #     ids = [s.rstrip('\n') for s in ids]

    # old_tweet = []
    # for id in ids[3000:]:
    #     old_t = mongo.table("tweet").find_one({"id_str": id}, {'_id': 0, 'retweet': 0})
    #     old_tweet.append(old_t)
    #
    # # for r in old_tweet:
    # #     dynamo.table("tweet").delete_item_silent({"S": r['id_str']})
    # dynamo.table("tweet").insert(old_tweet)
    # print(len(old_tweet))


    ids_bit = ids[15000:30000]
    rets = dynamo.table("tweet").find_batch(ids_bit)

    non_exists_ids = []
    for id_str in ids_bit:
        if len([r for r in rets if r["id_str"] == id_str]) == 0:
            non_exists_ids.append(id_str)

    if non_exists_ids:
        with open("non_exists_tweet_ids8.txt", "a") as f:
            f.write("\n".join(non_exists_ids) + "\n")


    # print(len(rets))


if __name__ == '__main__':
    tbl_tweet()