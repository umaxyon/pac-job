from common.util import Util


class TestTest(object):

    def test_get_hash(self):
        actual = Util.get_hash('hoge')
        actual2 = Util.get_hash('hoge')

        assert '-' not in actual
        assert actual == actual2

    def test_is_digit(self):
        assert Util.is_digit('1')
        assert Util.is_digit('-1')
        assert Util.is_digit('1.1')
        assert Util.is_digit('1.')
        assert not Util.is_digit('.1')
        assert not Util.is_digit(' 1')
        assert not Util.is_digit('1 ')
        assert not Util.is_digit('+1')
        assert not Util.is_digit('.')
        assert not Util.is_digit('-')

    def test_blank_to_none(self):
        test_data = {
            'ccode': '6090', 'info': '',
            'identify': ['hoge', '', 'huga', ''],
            'tweets': [
                {'id': '1', 'data': 'あああ'},
                {'id': '2', 'data': 'いいい'},
                {'id': '3', 'data': ''},
            ],
            'repo': {
                'id': 'repo_1',
                'date': '20180101',
                'items': ['a', '', 'b'],
                'name': ''
            }
        }
        expected = {
            'ccode': '6090', 'info': None,
            'identify': ['hoge', None, 'huga', None],
            'tweets': [
                {'id': '1', 'data': 'あああ'},
                {'id': '2', 'data': 'いいい'},
                {'id': '3', 'data': None},
            ],
            'repo': {
                'id': 'repo_1',
                'date': '20180101',
                'items': ['a', None, 'b'],
                'name': None
            }
        }
        Util.blank_to_none(test_data)
        assert test_data == expected
