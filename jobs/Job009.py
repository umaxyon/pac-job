import os
import boto3
import base64
from urllib import request
from common.dao import Dao
from common.log import tracelog
from common.pacjson import JSON
from common.util import Util

class Job009(object):
    """
    [job009]twitter_friendsのプロフィール画像を取得する。
    """

    def __init__(self, dao: Dao):
        self.dao = dao

    @tracelog
    def run(self, dao: Dao, h1):
        h1('twitter_friends取得')
        friends = dao.table('twitter_friends').full_scan()

        s3 = boto3.resource('s3').Bucket("kabupac.com")
        h1('画像取得してBase64化')
        err_list = []
        friend = None
        all_data = {}

        piece_up = False
        try:
            for fr in friends:
                friend = fr
                url = fr['profile_image_url']
                data = request.urlopen(url).read()
                str_img = base64.b64encode(data).decode('utf-8')
                # 個別
                json = {"i":"data:image/{};base64,{}".format(self.get_ext(url), str_img)}

                if piece_up:
                    s3.put_object(Key="prof/{}".format(fr['id_str']), Body=JSON.dumps(json))

                all_data[fr['id_str']] = "data:image/{};base64,{}".format(self.get_ext(url), str_img)
        except:
            Util.print_stack_trace()
            err_list.append(friend['id_str'])

        h1('s3にアップロード')
        s3.put_object(Key="prof/profimg.json", Body=JSON.dumps(all_data))

        print(err_list)
        h1('終了')

    def get_ext(self, url):
        ext = os.path.splitext(url)[1].lower()[1:]
        if ext in ('jpeg', 'jpg'):
            ext = 'jpeg'
        return ext