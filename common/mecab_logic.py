from common.dao import Dao
from common.log import Log

class MeCabLogic(object):
    def __init__(self):
        self.dao = Dao()

    def find_stock_code_in(self, mecab_dic):
        stocks_in_tweet, words = {}, []
        for word, meta in mecab_dic.items():
            Log.debug('「{}」 : {}', word, meta)
            if not meta.get('add1') == '株':
                continue

            b = self.find_brand_by_identify(word)
            if not b:
                continue
            # コードをキーにして、１ツイート内の同一ヒット単語をdistinct
            stocks_in_tweet[b["ccode"]] = b
            words.append({word: b["ccode"]})
        return stocks_in_tweet, words

    def find_brand_by_identify(self, word):
        """stock_identifyにつぶやきwordを当てて検索"""
        brands = self.dao.table('stock_identify').find_query({"nm": word})

        if not brands:
            Log.error('wordがstock_identifyに無い。mecab辞書とidentifyが不一致')
            return False
        if len(brands) > 1:
            brands_y = [b for b in brands if b['main'] == 'y']
            if len(brands_y) > 1:
                Log.error('mainで同じnmが複数登録されている: brands_y={}'.format(brands_y))
                return False
            elif len(brands_y) == 1:
                return brands[0]
            else:
                # ログだけ出して、複数nの１件目を使うことにする
                Log.warn('main=y がいない。nが複数。 brands={}'.format(brands))

        return brands[0]