import boto3
from common.dao import Dao
from common.log import tracelog
from common.twitter_inspector import TwitterInspector
from common.key.twitter_key import Irebaburn


class Job002(object):
    """
    [job002]Twitterからフォロー一覧取得してDBに取り込む。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):

        h1('フォロー一覧削除')
        self._delete_twitter_friends()

        h1('フォロー一覧取得')
        tw = TwitterInspector(Irebaburn)
        friends = tw.get_friends()

        h1('twitter_friendsコレクションに追加')
        dao.table('twitter_friends').insert(friends)

        h1('Job009をキック')
        boto3.client('batch').submit_job(
            jobName='Job009',
            jobQueue="arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job009_twitter_friends_profimg_create",
            jobDefinition="Job009_twitter_friends_profimg_create:1"
        )

    @tracelog
    def _delete_twitter_friends(self):
        self.dao.table('twitter_friends').delete_all()
