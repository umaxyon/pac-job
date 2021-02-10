import requests
import time
from urllib3.exceptions import ReadTimeoutError
from bs4 import BeautifulSoup
from common.async_worker import AsyncWorker
from common.log import Log


class Kabtan(object):

    def get_stock_detail(self, ccode):
        try:
            url = "https://kabutan.jp/stock/?code={}".format(ccode)
            html = requests.get(url, timeout=10)
            bs = BeautifulSoup(html.text, "lxml")
            buf = bs.find_all('table', attrs={"class": "stock_st_table"})
            if len(buf) < 2:
                return {'ccode': ccode}
        except ReadTimeoutError:
            return {'ccode': ccode, 'err': 'timeout'}

        tbl = buf[1]
        td = tbl.find_all('td', attrs={"class": "tar"})

        tan = td[5].text.replace(' 株', '').replace(',', '')
        jika = td[6].text.replace(' 億円', '').replace(',', '')

        ul = bs.select("div#main ul")[1]
        url = ul.select("li a")[0].text
        cate = ul.select("li dd")[2].text

        thema_list = ul.select("li")[3].select("dd a")
        themas = [r.text for r in thema_list]

        ret = dict()
        ret['ccode'] = ccode
        ret['url'] = url
        ret['cate'] = cate
        ret['thema'] = ','.join(themas)
        if not tan == '－' and not jika == '－':
            ret['tan'] = int(tan)
            ret['jika'] = float(jika)

        return ret

    def get_stock_details_async(self, ccodes):
        def _cb(results, code):
            return results

        start = 0
        end = min(len(ccodes), 200)
        ret = []
        while True:
            current_list = ccodes[start:end]
            Log.debug('kabtan request. len(ccodes)={}, ccodes[{}:{}]'.format(len(ccodes), start, end))
            result, err = AsyncWorker(self.get_stock_detail, _cb).go(current_list)
            if not result is None:
                ret.extend(result)
            Log.debug('len(ret)= {} '.format(len(ret)))
            time.sleep(10)
            start = end
            end = min(len(ccodes), end + 200)
            if end >= len(ccodes):
                return ret


if __name__ == '__main__':
    Kabtan().get_stock_details_async(['3808', '6090', '3845'])
    # Kabtan().get_stock_detail('6090')
