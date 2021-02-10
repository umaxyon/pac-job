from common.util import Util
from common.datetime_util import DateTimeUtil

from common.twitter_session import TwitterSession


class TwitterInspector(object):

    def __init__(self, key):
        self.session = TwitterSession(key)

    def get(self):
        return self.session.request("statuses/user_timeline",
                                    {'screen_name': 'muratamika2020',
                                     'count': '3200',
                                     'include_rts': '1'})
        # url = ("statuses/user_timeline"
        #        "?screen_name=muratamika2020&count=3200&include_rts=1")
        # res = self.session.get(url)
        # return json.loads(res)

    def get_timeline(self):
        timeline = self.session.request("statuses/user_timeline",
                                        {'count': 200, 'include_rts': 1})
        return timeline

    def get_list_id(self, list_name):
        list_list = self.session.get_list()
        print("list: {}".format(list_list))
        aaa = list(filter(lambda x: x['name'] == list_name, list_list))
        return aaa[0]['id_str']

    def get_list_timeline(self, list_id, count=200, max_id=None):
        param = {'list_id': list_id, 'count': count, 'include_rts': 1, 'tweet_mode': 'extended'}
        if max_id:
            param['max_id'] = max_id
        return self.session.request("lists/statuses", param)

    def get_list_timeline_rotate(self, list_name, count, last_data_id=None):
        list_id = self.get_list_id(list_name)
        cnt = min(count, 200)
        max_id = None
        timeline = []
        while True:
            cnt = count - len(timeline)
            if timeline and cnt <= 1:
                break
            tweetlist = self.get_list_timeline(list_id, cnt, max_id)
            if tweetlist[0]['id_str'] == max_id:
                timeline.pop()  # 重複を取り除く

            last_idx = Util.find_index_dict_in_list('id_str', last_data_id, tweetlist)
            if last_idx == 0:
                break  # 取得データの先頭が保存済み

            if last_idx > 0:
                tweetlist = tweetlist[:last_idx]  # 保存済み以降のデータを切り捨て

            timeline.extend(tweetlist)
            lastid = timeline[-1]['id_str']  # 最後の1件のid
            if lastid == max_id:
                break
            max_id = lastid

        return self.to_simple_tweet_list(timeline)

    def to_simple_tweet_list(self, tweet_list):
        timeline_list = []
        for t in tweet_list:
            dup = [x for x in timeline_list if x['id_str'] == t['id_str']]
            if dup:
                # 重複除去
                continue

            dat = {}
            dat['id_str'] = t['id_str']
            dat['text'] = t['full_text']
            dat['created_at'] = DateTimeUtil.strf_ymdhms(DateTimeUtil.date_from_utc(t['created_at']))

            user = t.get('user')
            dat['user_id'] = user['id_str']
            dat['user_name'] = user['name']
            dat['user_screen_name'] = user['screen_name']
            retweet = t.get("retweeted_status")
            if retweet:
                dat['retweet_id'] = retweet['id_str']
                dat['retweet_user_id'] = retweet['user']['id_str']
                dat['retweet_user_name'] = retweet['user']['name']
                dat['retweet_user_profile_image'] = retweet['user']['profile_image_url']
                dat['retweet_text'] = retweet['full_text']

            media = t.get('entities').get('media')
            if media:
                m_url_list = []
                for m in media:
                    m_url = m.get('media_url_https')
                    if m_url:
                        m_url_list.append(m_url)
                dat['media_url'] = m_url_list

            timeline_list.append(dat)
        return timeline_list

    def get_friends(self):
        friends = self.session.request("friends/list", {'count': 200})

        friend_list = []
        for f in friends['users']:
            friend_list.append({k: f[k] for k in ['id_str', 'screen_name', 'name', 'url', 'profile_image_url']})

        return friend_list
