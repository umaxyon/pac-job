# coding:utf-8
import re
import traceback
import sys
import types
import hashlib
from datetime import datetime

import emoji
from dateutil import parser
from pytz import timezone

from common import const
from common.norm import normalize_neologd
from common.datetime_util import DateTimeUtil

class Util(object):
    @staticmethod
    def print_stack_trace():
        e = sys.exc_info()
        tb_info = traceback.format_tb(e[2])
        print("error!! {}".format(e))
        for tb in tb_info:
            print(tb)

    @staticmethod
    def is_digit(str_val):
        if re.match("^-?\d+\.?\d*?$", str_val) is None:
            return False
        elif str_val == '.' or str_val == '-':
            return False
        else:
            try:
                val = float(str_val)
                return True
            except ValueError:
                return False

    @staticmethod
    def get_hash(str_val):
        return hashlib.md5(str_val.encode('utf-8')).hexdigest()

    @staticmethod
    def get_propname_list(ob):
        return list(filter((lambda r: not r.startswith('__')), dir(ob)))

    @staticmethod
    def to_simple_dict(ob):
        dic = {}
        for prop in Util.get_propname_list(ob):
            val = getattr(ob, prop)
            if not Util.is_method_type(val):
                dic[prop] = val
        return dic

    @staticmethod
    def to_simple_dict_list(ob):
        lst = []
        for b in ob:
            lst.append(Util.to_simple_dict(b))
        return lst

    @staticmethod
    def to_date(str_date):
        return datetime.strptime(str_date, '%Y-%m-%d')

    @staticmethod
    def to_date_from_utc(str_date):
        return parser.parse(str_date).astimezone(timezone('Asia/Tokyo'))
        # return datetime.strptime(str_date, '%a %b %d %H:%M:%S +0000 %Y')

    @staticmethod
    def is_method_type(ob):
        return type(ob) is types.MethodType

    @staticmethod
    def get_called_func_info():
        fr = sys._getframe().f_back
        while True:
            if fr.f_code.co_name not in ('info', 'debug', 'warn', 'error', 'critical', '_wrap'):
                break

            fr = fr.f_back
        return fr.f_globals['__name__'], fr.f_lineno

    @staticmethod
    def find_index_dict_in_list(key, val, dict_in_list):
        """ 辞書入りリスト内の指定キーの値を含む辞書のリストindexを返す。ない場合は -1。"""
        for i in range(len(dict_in_list)):
            if dict_in_list[i][key] == val:
                return i
        return -1

    @staticmethod
    def remove_emoji(str_text):
        """絵文字を除去"""
        return ''.join(c for c in str_text if c not in emoji.UNICODE_EMOJI)

    @staticmethod
    def remove_kaomoji(str_text):
        """ TODO """
        return str_text

    @staticmethod
    def remove_url(str_text):
        """文字列中のURL部を削除する。削除したURLをリストにし、削除文字列とURLリストをタプルで返す"""
        url_list = []

        def _urlmatch(m):
            url_list.append(m.group())
            return ''

        ret_text = re.sub(const.PTN_URL, _urlmatch, str_text)
        # for mm in re.finditer(const.PTN_URL, str_text):
        #     url_list.append(mm.group())
        return ret_text, url_list

    @staticmethod
    def remove_crlf(str_text):
        """改行を削除する"""
        text = str_text.replace('\r', '')
        text = text.replace('\n', '')
        return text

    @staticmethod
    def remove_useless(str_text):
        text = re.sub(r'^>', '', str_text)
        text = re.sub(r'Rttwname:', 'Rttwname', str_text)
        return text

    @staticmethod
    def mask_twitter_name(str_text):
        ptn = r'@[0-9a-zA-Z_]+'
        return re.sub(ptn, 'twname', str_text)

    @staticmethod
    def remove_overlap(str_text):
        text = re.sub(r'!+', '!', str_text)
        text = re.sub(r'W+', 'w', text)
        text = re.sub(r'w+', 'w', text)
        text = re.sub(r'\?+', '?', text)
        text = re.sub(r'♬+', '♪', text)
        text = re.sub(r'♪+', '♪', text)
        text = re.sub(r'☆+', '★', text)
        text = re.sub(r'★+', '★', text)
        return text

    @staticmethod
    def all_normalize(str_text):
        text = normalize_neologd(str_text)
        text = Util.remove_emoji(text)
        text = Util.remove_crlf(text)
        text, url_list = Util.remove_url(text)
        text = Util.remove_useless(text)
        text = Util.remove_kaomoji(text)
        text = Util.remove_overlap(text)
        return text

    @staticmethod
    def blank_to_none(item):
        if type(item) is dict:
            for key in item:
                buf = item[key]
                if type(buf) in (dict, list):
                    Util.blank_to_none(buf)
                if type(buf) is str and buf == '':
                    item[key] = None
        elif type(item) is list:
            for i in range(len(item)):
                row = item[i]
                if type(row) in (dict, list):
                    Util.blank_to_none(row)
                if type(row) is str and row == '':
                    item[i] = None

    @staticmethod
    def date_to_str(item):
        if type(item) is dict:
            for key in item:
                buf = item[key]
                if type(buf) in (dict, list):
                    Util.blank_to_none(buf)
                if type(buf) is datetime:
                    item[key] = DateTimeUtil.strf_ymdhms(buf)
        elif type(item) is list:
            for i in range(len(item)):
                row = item[i]
                if type(row) in (dict, list):
                    Util.blank_to_none(row)
                if type(row) is datetime:
                    item[i] = DateTimeUtil.strf_ymdhms(row)