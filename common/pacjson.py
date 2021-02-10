# coding:utf-8
import json
from datetime import datetime

_DATE_FORMAT = '%Y/%m/%d_%H:%M:%S'


class DateTimeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.strftime(_DATE_FORMAT)
        return super(DateTimeJsonEncoder, self).default(o)


class JSON(object):

    @staticmethod
    def dumps(target):
        return json.dumps(target, cls=DateTimeJsonEncoder)

    @staticmethod
    def loads(target, date_field=[]):
        dat = json.loads(target, encoding='UTF-8')

        def _recurcive(key, ob):
            if '.' in key:
                buf = key.split('.')
                k = buf.pop(0)
                if isinstance(ob[k], list):
                    for r in ob[k]:
                        _recurcive('.'.join(buf), r)
                else:
                    _recurcive('.'.join(buf), ob[k])
            else:
                ob[key] = datetime.strptime(ob[key], _DATE_FORMAT)

        for f in date_field:
            _recurcive(f, dat)
        return dat
