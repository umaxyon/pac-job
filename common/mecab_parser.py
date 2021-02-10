# coding:utf-8
import re
import boto3
import json
from common.const import PTN_URL
from common.const import PTN_FIGURE


class RemoteMecabParser(object):
    def parse(self, text):

        client_lambda = boto3.client("lambda")
        resp = client_lambda.invoke(
            FunctionName="arn:aws:lambda:ap-northeast-1:007575903924:function:MecabFunc",
            InvocationType="RequestResponse",
            Payload=json.dumps({"sentence": text}))
        ret = json.loads(resp['Payload'].read())
        return ret['tokens']


class MeCabParser(object):
    def __init__(self):
        self.engine = RemoteMecabParser()

    def parse(self, text):
        masked_text, urls = self.url_mask(text)
        masked_text, figures = self.figure_mask(masked_text)
        masked_text, digits = self.digit_mask(masked_text)

        tokens = self.engine.parse(masked_text)
        dic = {}
        for t in tokens:
            key = t['surface']
            f = t['feature']
            buf = f.split(',')
            meta = {'part1': buf[0], 'part2': buf[1], 'part3': buf[2]}
            if len(buf) > 9:
                meta['add1'] = buf[9]
            if key == 'PACMECABURL':
                key = urls.pop(0)
            if key == 'PACMECABFIGURE':
                key = figures.pop(0)
            if key == 'PACMECABDIGIT':
                key = digits.pop(0)
            dic[key] = meta  # 同じwordはキーとしてdistinctされる

        return dic

    def url_mask(self, text):
        match = re.findall(PTN_URL, text)
        for m in match:
            text = text.replace(m, 'PACMECABURL')

        return text, match

    def figure_mask(self, text):
        match_list = []
        for ptn in PTN_FIGURE:
            match = re.findall(ptn, text)
            for m in match:
                text = text.replace(m, 'PACMECABFIGURE')
            match_list += match
        return text, match_list

    def digit_mask(self, text):
        match = re.findall('\d{5,}', text)
        for m in match:
            text = text.replace(m, 'PACMECABDIGIT')
        return text, match
