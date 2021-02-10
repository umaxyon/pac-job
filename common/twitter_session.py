from requests_oauthlib import OAuth1Session
from urllib import parse
import json

TWITTER_BASE_URL = 'https://api.twitter.com/1.1/'


class TwitterSession(object):
    def __init__(self, key):
        self.session = OAuth1Session(
            key.CK.value, key.CS.value, key.AT.value, key.AS.value)
        self.key = key

    def request(self, command, param=None):
        """ TwitterApiに問い合わせてJsonを返す。"""
        url = TWITTER_BASE_URL + command + '.json'

        if param:
            url += '?' + parse.urlencode(param)

        result = self.session.get(url)
        return json.loads(result.text)

    def get_list(self, screan_name=None):
        """ 指定ユーザーのリスト一覧取得"""
        return self.request("lists/list", {"screan_name": screan_name or self.key.name.value})

    def get_list_timeline(self, list_id, max_id=None):
        """ 指定リストのリストタイムラインを取得 """
        param = {'list_id': list_id, 'count': 200, 'include_rts': 1}
        if max_id:
            param['max_id'] = max_id
        return self.session.request("lists/statuses", param)
