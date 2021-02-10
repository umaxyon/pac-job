import pandas as pd

from common.mecab_parser import MeCabParser


class TestMecabParser(object):
    def setup_method(self, method):
        print('method={}'.format(method.__name__))
        self.mecab = MeCabParser()

    # @pytest.mark.skipif("True")
    def test_mecab1(self):
        """ 証券コードと会社名を含む文章 """
        text = '8135ゼット いいっすよね'
        actual = self.mecab.parse(text)
        print(pd.DataFrame(actual).T)
        assert count_word(actual, '8135') == 1
        assert get_add1(actual, '8135') == '株'
        assert count_word(actual, 'ゼット') == 1
        assert get_add1(actual, 'ゼット') == '株'

    # @pytest.mark.skipif("True")
    def test_mecab2(self):
        """ URLを含む文章 """
        text = '【上方修正】候補リスト ＜成長株特集  12月17日記事https://t.co/ZzvlEX74Fv割安、四季… https://t.co/zcPTo33yOe'
        actual = self.mecab.parse(text)
        print(pd.DataFrame(actual).T)
        assert count_word(actual, 'https://t.co/ZzvlEX74Fv') == 1
        assert count_word(actual, 'https://t.co/zcPTo33yOe') == 1

    def test_mecab3(self):
        """ 金額、個数、回数を含む文章 """
        text = '8410 セブン銀行は今季8410億の特別損失を計上する予定。8410回目。'
        actual = self.mecab.parse(text)
        print(pd.DataFrame(actual).T)
        assert count_word(actual, '8410') == 1
        assert count_word(actual, '8410億') == 1
        assert count_word(actual, '8410回') == 1

        text = '6090約6090万人$3823、\\4,875およそ6467個のべ5955万人約3920兆円-9,418億点で1,233,808万ドル123億0万円55'
        actual = self.mecab.parse(text)
        print(pd.DataFrame(actual).T)
        assert count_word(actual, '6090') == 1
        assert get_add1(actual, '6090') == '株'
        assert count_word(actual, '$3823') == 1
        assert count_word(actual, '\\4,875') == 1
        assert count_word(actual, '6467個') == 1
        assert count_word(actual, '5955万人') == 1
        assert count_word(actual, '約3920兆円') == 1
        assert count_word(actual, '-9,418億点') == 1
        assert count_word(actual, '1,233,808万ドル') == 1
        assert count_word(actual, '123億') == 1
        assert count_word(actual, '0万円') == 1

        text = '2300超えてきた。6090以上行くでしょ。1980以下で仕込んだ。2300突破オメ。8500通過。1000%確実。'
        actual = self.mecab.parse(text)
        print(pd.DataFrame(actual).T)
        assert count_word(actual, '2300超え') == 1
        assert count_word(actual, '6090以上') == 1
        assert count_word(actual, '1980以下') == 1
        assert count_word(actual, '2300突破') == 1
        assert count_word(actual, '8500通過') == 1
        assert count_word(actual, '1000%') == 1


def count_word(parsed_dict, word):
    keys = list(parsed_dict.keys())
    return len([k for k in keys if k == word])


def get_add1(parsed_dict, word):
    return parsed_dict[word]['add1']
