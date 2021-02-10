import enum
import re
from collections import OrderedDict
from datetime import datetime

import requests
import urllib3
import zenhan
from bs4 import BeautifulSoup
from common.datetime_util import DateTimeUtil
from common.log import Log
from common.dao import Dao
from common.util import Util
from requests.sessions import Session
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)


class EdinetUrl(enum.Enum):
    search_url = ('https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?'
                  'uji.verb=W1E63031Search&uji.bean=ee.bean.W1E63030.EEW1E63031Bean&'
                  'PID=W1E63031&TID=W1E63031&SESSIONKEY=1519228694176&stype=0&dcdSelect=12001&hcdSelect=01001&'
                  'ycdSelect=&tsbSdt=&syoruiKanriNo=&keyword1=&keyword2=&keyword3=&keyword4=&keyword5=&lgKbn=2&'
                  'pkbn=0&skbn=1&dskb=&askb=&dflg=0&iflg=0&preId=1&chr=%E5%A4%A7%E6%A0%AA%E4%B8%BB&hbn=true&spf5=2&'
                  'otd=12001&otd=13001&otd=14001&otd=15001&otd=16001&otd=17001&'
                  'hcd=01001&ycd=&sec=&scc=&snm=&spf1=1&spf2=1&iec=&icc=&inm=&spf3=1&fdc=&fnm=&'
                  'spf4=1&cal=1&era=H&yer=&mon=&cal2=2&psr=2&yer2={}&mon2={}&day2={}&yer3={}&mon3={}&day3={}&'
                  'row=100&idx={}&str=&kbn=1&flg=')

    frame_initialize_url = (
        'https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W00Z1010initialize&'
        'uji.bean=ek.bean.EKW00Z1010Bean&PID=W1E63031&TID=W00Z1010&SESSIONKEY=1519227492822&stype=0&'
        'dcdSelect=12001&hcdSelect=01001&ycdSelect=03001400&tsbSdt=B_01040107&syoruiKanriNo={}&'
        'keyword1=%E5%A4%A7%E6%A0%AA%E4%B8%BB&keyword2=&keyword3=&keyword4=&keyword5=&lgKbn=2&pkbn=0&'
        'skbn=1&dskb=&askb=&dflg=0&iflg=0&preId=1&chr=%E5%A4%A7%E6%A0%AA%E4%B8%BB&hbn=true&spf5=2&'
        'otd=12001&hcd=01001&ycd=03001400&sec=&scc=&snm=&spf1=1&spf2=1&iec=&icc=&inm=&spf3=1&fdc=&fnm=&'
        'spf4=1&cal=1&era=H&yer=&mon=&psr=1&pid=4&row=100&idx=0&str=&kbn=1&flg=')

    header_frame_url = ('https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?'
                        'PID=W00Z1010&syoruiKanriNo={}&publicKbn=1&riyousyaKbn=E&SESSIONKEY=&'
                        'privateDocumentIndicateFlg=&teisyutuEngCheckResult=false&'
                        'keyword1=%E5%A4%A7%E6%A0%AA%E4%B8%BB&keyword2=&keyword3=&keyword4=&keyword5=&'
                        'be.keyword=%E5%A4%A7%E6%A0%AA%E4%B8%BB&be.keyword=&be.keyword=&be.keyword=&'
                        'be.keyword=&lgKbn=2&uji.verb=W00Z1010init&uji.bean=ek.bean.EKW00Z1010Bean&TID=W00Z1010_10')

    menu_url = ('https://disclosure.edinet-fsa.go.jp/E01EW/download?{}&uji.verb=W00Z1010Document&'
                'be.bean.id={}&be.target=1&be.request=0&SESSIONKEY=&'
                'be.keyword=%E5%A4%A7%E6%A0%AA%E4%B8%BB&PID=W00Z1010')

    data_url = ('https://disclosure.edinet-fsa.go.jp/E01EW/download?{}&uji.verb=W00Z1010Document&'
                'be.bean.id={}&be.target={}&be.request=1&SESSIONKEY=&'
                'be.keyword=&PID=W00Z1010')


class Edinet(object):

    def __init__(self):
        self.user_agent = {
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    def get_report_search_results(self, start_day: datetime, end_day: datetime, start_page=1, page_count=None):
        """
        EDINETの書類検索（全文検索）の結果一覧を取得する。
        対象は「有価証券報告書」「文字列に『大株主の状況』を含む」「指定期間」
        検索一覧行オブジェクトリストを返す。
        start_page(1始まり)、page_countの指定がない場合は全ページ対象。
        """
        rows, idx, page = [], 0, 1
        while True:
            url = EdinetUrl.search_url.value.format(
                start_day.year, start_day.month, start_day.day,
                end_day.year, end_day.month, end_day.day, str(idx))
            html = requests.get(url, timeout=20, headers=self.user_agent, verify=False)
            bs = BeautifulSoup(html.text, "lxml")

            if start_page <= page:
                table = bs.find_all('table', {'class': 'resultTable'})
                row_datas = self.get_row_datas(table)
                rows.extend(row_datas)

                Log.debug('{}ページ読込。計{}件'.format(page, len(rows)))

            if page_count and page_count == page + 1 - start_page:
                break

            if not self.has_next_page(bs):
                break

            idx = idx + 100
            page = page + 1

        return rows

    def scraping_securities_report(self, search_row):
        """
        検索一覧行オブジェクトを基に有価証券報告書をスクレイピングする。
        結果辞書オブジェクトを返す。
        """
        data_html = self.get_report_html(search_row['syorui_kanri_no'])
        Log.info('html取得完了 : {}'.format(search_row['company_name']))
        if '大株主' not in data_html:
            Log.warn('HTMLに大株主が無い。別資料かも。 company_name : {}'.format(search_row['company_name']))
            return None

        table_list = self.report_html_to_split_table_list(data_html)
        if not table_list:
            Log.warn('HTMLがフォーマットに合わない。 company_name : {}'.format(search_row['company_name']))
            return None

        Log.info('HTML -> table分割 : {}'.format(search_row['company_name']))
        dat = {}
        for t in table_list:
            if t['title'] == '発行済株式' and 'outstanding_share' not in dat:
                dat['outstanding_share'] = self.outstanding_share(t, search_row)
                Log.debug('** 発行済株式 : {}'.format(search_row['company_name']))

            if t['title'] == '所有者別状況':
                dat['holder_rate'] = self.holder_rate_status(t, search_row)
                Log.debug('** 所有者別状況 : {}'.format(search_row['company_name']))

            if t['title'] == '大株主の状況':
                dat['holders'] = self.major_shareholders(t, search_row)
                Log.debug('** 大株主の状況 : {}'.format(search_row['company_name']))

        dat['date'] = search_row['submition_day']
        if 'outstanding_share' in dat and 'holder_rate' in dat:
            if '単元' in dat['holder_rate']['unit']:
                dat['holder_rate']['unit'] = dat['outstanding_share']['unit']
            elif '株' in dat['holder_rate']['unit']:
                dat['holder_rate']['unit'] = 1

        return dat

    def get_row_datas(self, table):
        """書類検索結果の表から行データを抜き出す。"""
        if not table:
            return []

        trs = table[0].select('tr')
        ret = []
        for i in range(1, len(trs), 2):
            dat = {
                'submition_day': trs[i].td.div.text.strip(),
                'syorui_kanri_no': re.search("'(.+)'", trs[i].find_all('td')[1].a.get('onclick')).group(1),
                'company_name': trs[i].find_all('td')[4].a.text.replace('株式会社', '(株)'),
                'syorui_mei': trs[i].find_all('td')[1].text.replace('\n', '')
            }
            ret.append(dat)
        return ret

    def get_stock_brands_from_search_row(self, search_row, dao: Dao):
        """検索一覧行オブジェクトを基にstock_brandsを検索する"""
        name = self.company_name_normarize(search_row['company_name'])
        name_s = self.norm(name, blank_cut=False)
        brand = dao.table('stock_brands').find_one({
            '$or': [
                {'name': re.compile(name)}, {'name': re.compile(name_s)},
                {'identify': {"$in": [name, name_s]}}]
        })
        return brand

    def has_next_page(self, bs: BeautifulSoup):
        """書類検索結果のページング判定"""
        paging_links = paging_last = bs.select('#pageTop a')
        return paging_links and paging_links[-1].text == '次ページ'

    def get_mainframe_request_key(self, s: Session, syorui_kanri_no):
        """書類閲覧フレームのイニシャライズHTMLからヘッダ取得用GETパラメータにあたるキーを取得する。"""

        url = EdinetUrl.frame_initialize_url.value.format(syorui_kanri_no)
        html = s.get(url, timeout=10, headers=self.user_agent, verify=False)

        bs = BeautifulSoup(html.text, "lxml")
        return bs.select("[name=viewFrame]")[0].get('src')

    def get_headerframe_bean_id(self, s: Session, syorui_kanri_no):
        """書類閲覧フレームのヘッダーHTMLからbeanidを取得する。"""

        url = EdinetUrl.header_frame_url.value.format(syorui_kanri_no)
        header_html = s.get(url, timeout=10, headers=self.user_agent, verify=False)
        header_lines = header_html.text.split('\r\n')
        beanid_line = [line for line in header_lines if line.find('"be.bean.id"') >= 0][0]
        return re.search('"be.bean.id", "(.+)"', beanid_line).group(1)

    def get_menuframe_doc_request_key(self, s: Session, main_frame_request_key, bean_id):
        """書類閲覧フレームのメニューHTMLからドキュメントリクエスト用GETパラメータにあたるキーを取得する。"""
        url = EdinetUrl.menu_url.value.format(main_frame_request_key, bean_id)
        menu_html = s.get(url, timeout=10, headers=self.user_agent, verify=False)
        menu_html_lines = menu_html.text.split('\n')
        getpara_line = [line for line in menu_html_lines if line.find('/download?') >= 0][0]
        ookabu_line = [line for line in menu_html_lines if line.find('大株主') >= 0 and line.find('の状況') >= 0]
        ookabu_target = '7'
        if len(ookabu_line) != 0:
            m = re.search('doAction\(.+,.+, \'(\d)\'', ookabu_line[0])
            if m:
                ookabu_target = m.group(1)
        return re.search('download\?(\d+)"', getpara_line).group(1), ookabu_target

    def get_report_html(self, syorui_kanri_no):
        """有価証券報告書のHTMLを取得する。"""
        s = requests.Session()

        main_frame_request_key = self.get_mainframe_request_key(s, syorui_kanri_no)
        bean_id = self.get_headerframe_bean_id(s, syorui_kanri_no)
        doc_request_key, ookabu_target = self.get_menuframe_doc_request_key(s, main_frame_request_key, bean_id)

        url = EdinetUrl.data_url.value.format(doc_request_key, bean_id, ookabu_target)
        data_html = s.get(url, timeout=10, headers=self.user_agent, verify=False)
        return data_html.text

    def report_html_to_split_table_list(self, html, shorui_mei):
        """有価証券報告書HTMLをタイトル別テーブルリストに分割する"""
        html = html.replace('&#160;', ' ').replace('　', ' ')
        data_html_list = html.split('\n')
        dat, current, current_flg, hit_flg, find_flg = [], {}, False, False, False
        tbl_cnt, tr_buf, tr_flg = 0, '', False

        shihan = re.search('四半期', shorui_mei)
        teisei = re.search('訂正', shorui_mei)

        # (タイトル, table数)
        titles = [('①【株式の総数】', 1,),
                  ('②【発行済株式】', 1,),
                  ('（５）【発行済株式総数、資本金等の推移】', 1,),
                  ('（６）【所有者別状況】', 2,),
                  ('（６）【大株主の状況】', 1,),
                  ('（７）【大株主の状況】', 1,),
                  ('①【発行済株式】', 2,),
                  ('②【自己株式等】', 2,),
                  ('（１）【最近５年間の事業年度別最高・最低株価】', 1,),
                  ('（２）【最近６月間の月別最高・最低株価】', 1,)]

        title_no = -1
        for i, html_row in enumerate(data_html_list):
            if not current_flg:
                hit_flg, current = self.hit_title(html_row, titles, 0)
                if hit_flg: current_flg = True;title_no = 0; continue
                hit_flg, current = self.hit_title(html_row, titles, 1)
                if hit_flg: current_flg = True;title_no = 1;  continue
                hit_flg, current = self.hit_title(html_row, titles, 2)
                if hit_flg: current_flg = True;title_no = 2;  continue
                hit_flg, current = self.hit_title(html_row, titles, 3)
                if hit_flg: current_flg = True;title_no = 3;  continue
                hit_flg, current = self.hit_title(html_row, titles, 4)
                if hit_flg: current_flg = True;title_no = 4;  continue
                hit_flg, current = self.hit_title(html_row, titles, 5)
                if hit_flg: current_flg = True;title_no = 5;  continue
                hit_flg, current = self.hit_title(html_row, titles, 6)
                if hit_flg: current_flg = True;title_no = 6;  continue
                hit_flg, current = self.hit_title(html_row, titles, 7)
                if hit_flg: current_flg = True;title_no = 7;  continue
                hit_flg, current = self.hit_title(html_row, titles, 8)
                if hit_flg: current_flg = True;title_no = 8;  continue
                hit_flg, current = self.hit_title(html_row, titles, 9)
                if hit_flg: current_flg = True;title_no = 9;  continue

            html_row = self.norm(html_row)
            if 'dec_date' not in current:
                nontag = self.tag_remove(html_row)
                if re.search('..\d?\d年\d+?月\d+?日現在', nontag):
                    current['dec_date'] = nontag

            if current_flg and self.find_text(html_row, '<table'):
                find_flg = True

            if find_flg:
                html_row = self.table_html_style_cut(html_row)
                if html_row:
                    # <tr> タグの内容が複数行に跨る場合にtr_buf変数で１行にまとめる
                    if self.find_text(html_row, '<tr'):
                        tr_flg = True
                    if tr_flg:
                        tr_buf = tr_buf + html_row
                        if self.find_text(tr_buf, '</tr>'):
                            html_row, tr_flg = tr_buf, False
                            tr_buf = ''
                        else:
                            continue
                if i < len(data_html_list) and titles[title_no][1] > 1:
                    # 複数テーブルまとめ食いパターンの場合、1行先読みして
                    # テーブル閉じより先に次のタイトル登場しないかチェック
                    try:
                        h, c = self.hit_title(data_html_list[i + 1], titles, title_no + 1)
                        if h:
                            # 会社によって複数テーブルだったり1テーブルだったりするパターン。
                            # (ここでhitしたことにして、current続行)
                            dat.append(current)
                            current = c
                            title_no = title_no + 1
                            tbl_cnt = 0
                    except:
                        # フォーマットに合わない。
                        if dat:
                            return dat
                        return []

                if html_row:
                    current['table'].append(html_row)

                if self.find_text(html_row, '</table>'):
                    tbl_cnt = tbl_cnt + 1
                    if tbl_cnt == titles[title_no][1]:
                        tbl_cnt = 0
                        find_flg = False
                        current_flg = False
                        dat.append(current)
                        if title_no + 1 == len(titles):
                            break
            if not shihan and not teisei and i >= len(data_html_list) - 1:
                # フォーマットに合わない。
                return []
        return dat

    def table_html_style_cut(self, html):
        html = re.sub(r'style=".+?"', "", html)
        html = re.sub(r'valign=".+?"', "", html)
        html = re.sub(r'class=".+?"', "", html)
        html = re.sub(r'colspan=".+?"', "", html)
        html = re.sub(r'rowspan=".+?"', "", html)
        html = re.sub(r'cellspacing=".+?"', "", html)
        html = re.sub(r'cellpadding=".+?"', "", html)
        html = re.sub(r'name=".+?"', "", html)
        html = re.sub(r'ref=".+?"', "", html)
        html = re.sub(r'<br/>', "", html)
        html = re.sub(r'<p>', "", html)
        html = re.sub(r'</p>', "", html)
        html = re.sub(r'<colgroup>', "", html)
        html = re.sub(r'</colgroup>', "", html)
        html = re.sub(r'<col/>', "", html)
        html = re.sub(r'<td/>', "", html)
        html = re.sub(r'</ix:.+?>', "", html)
        html = re.sub(r'<ix:.+?>', "", html)

        html = self.norm(html, blank_cut=False)
        return html

    def hit_title(self, row, titles, title_cnt):
        title = titles[title_cnt][0]
        hit_flg, doc = False, {}
        if self.norm(row).find(self.norm(title)) >= 0:
            t = re.search('【(.+)】', title).group(1)
            doc = {'title': t, 'table': []}
            hit_flg = True
        return hit_flg, doc

    def outstanding_share(self, t, search_row):
        """発行済株式"""
        ret = {}
        for r in t['table']:
            td_cnt = r.count('<td>')
            if td_cnt > 0:
                m = re.search('<td>(.*?)</td>' * td_cnt, r)
                if m:
                    ttl = self.tag_remove(m.group(1))
                    if '種類' in ttl:
                        buf = self.norm(self.tag_remove(m.group(3)))
                        if '年' in buf and '月' in buf and '日' in buf:
                            ret['date'] = self.convert_date(buf)

                    if '普通株式' in ttl:
                        try:
                            str_hakkou = self.tag_remove(m.group(3)).replace(',', '')
                            ret['hutuu_hakkou'] = str_hakkou if not Util.is_digit(str_hakkou) else str(int(str_hakkou.split('.')[0]))

                            mm = re.search('([\d,]+)株', m.group(5))
                            if mm:
                                str_tan = mm.group(1).replace(',', '')
                                ret['unit'] = str_tan if not Util.is_digit(str_tan) else str(int(str_tan))
                            else:
                                ret['unit'] = str(1)
                        except:
                            print('outstanding_share: 普通株式 err')

        if 'date' not in ret or ret['date'] == '-':
            Log.warn('発行済株式日付不正: {}'.format(search_row['company_name']))
            return '-'
        if 'hutuu_hakkou' not in ret:
            Log.warn('発行済株式数不正: {}'.format(search_row['company_name']))
            return '-'
        if 'unit' not in ret:
            Log.warn('発行済株式単元数不正: {}'.format(search_row['company_name']))
            return '-'
        return ret

    def holder_rate_status(self, t, search_row):
        """所有者別状況"""
        # gov: 政府及び地方公共団体, fin: 金融機関, sec: 金融商品取引業者, cop: その他法人
        # frc: 外国法人, frp: 外国個人, otp:個人その他, tot: 計, lwu: 単元未満株式
        col_flg = OrderedDict({
            'gov': False, 'fin': False, 'sec': False, 'cop': False,
            'frc': False, 'frp': False, 'otp': False, 'tot': False, 'lwu': False
        })
        ret = {
            'gov': [], 'fin': [], 'sec': [], 'cop': [],
            'frc': [], 'frp': [], 'otp': [], 'tot': [], 'lwu': []
        }
        reach_kabunushi_su = False

        for r in t['table']:
            td_cnt = r.count('<td>')
            if td_cnt > 0:
                m = re.search('<td>(.*?)</td>' * td_cnt, r)
                if m:
                    if not reach_kabunushi_su:
                        for i in range(td_cnt):
                            row = self.tag_remove(m.group(i + 1)).replace(' ', '')
                            if not col_flg['gov'] and '政府及び地方公共団体' in row:
                                col_flg['gov'] = True
                            if not col_flg['fin'] and '金融機関' in row:
                                col_flg['fin'] = True
                            if not col_flg['sec'] and ('金融商品取扱業者' in row or '金融商品取引業者' in row):
                                col_flg['sec'] = True
                            if not col_flg['cop'] and 'その他の法人' in row:
                                col_flg['cop'] = True
                            if not col_flg['frc'] and '個人以外' in row:
                                col_flg['frc'] = True
                            if not col_flg['frp'] and '個人' == row:
                                col_flg['frp'] = True
                            if not col_flg['otp'] and '個人その他' in row:
                                col_flg['otp'] = True
                            if not col_flg['tot'] and '計' in row:
                                col_flg['tot'] = True
                            if not col_flg['lwu'] and '単元未満株式の状況' in row:
                                col_flg['lwu'] = True

                            if '株主数' in row:
                                reach_kabunushi_su = True
                                break

                    if reach_kabunushi_su:
                        kbn = self.tag_remove(m.group(1))

                        if re.search(r'所有株式数\s*\(', kbn):
                            ret['unit'] = kbn.replace('所有株式数', '')

                        if '株主数' in kbn or '所有株式数' in kbn or kbn in '所有株式数の割合':
                            row_cnt = 2
                            for k, v in col_flg.items():
                                if v:
                                    if row_cnt < len(m.groups()):
                                        val = self.tag_remove(m.group(row_cnt)).replace(',', '')
                                        val = self.norm(val)
                                        val = '0' if val == '－' or val == '-' or val == '―' else val
                                        if Util.is_digit(val):
                                            ret[k].append(str(float(val)))
                                        else:
                                            ret[k].append(str(0))
                                        row_cnt = row_cnt + 1

        if 'dec_date' not in t:
            Log.warn('所有者別日付不正: {}'.format(search_row['company_name']))
            return '-'
        if 'unit' not in ret:
            Log.warn('株式単位不正: {}'.format(search_row['company_name']))
            return '-'

        ret['date'] = self.convert_date(t['dec_date'])
        return ret

    def major_shareholders(self, t, search_row):
        """大株主の状況"""
        holders, vol_unit = [], ''
        for r in t['table']:
            if not vol_unit and self.find_text(r, '所有株式数'):
                vol_unit = self.tag_remove(r).replace('所有株式数', '')

            m = re.search('<td>(.+?)</td><td>(.+?)</td><td>(.+?)</td><td>(.+?)</td>', r)
            if not m or '住所' in m.group(2):
                continue

            holder = self.tag_remove(m.group(1))
            holder = re.sub('\(常任代理人.+$', '', holder)
            vol = self.tag_remove(m.group(3)).replace(',', '')
            rate = self.tag_remove(m.group(4)).replace(',', '')

            if Util.is_digit(vol):
                vol = re.sub('\.\d+$', '', vol)
                vol = str(int(vol))
            if Util.is_digit(rate):
                rate = str(float(rate))

            holders.append([holder, vol, rate])

        if 'dec_date' not in t:
            Log.warn('大株主報告日付不正: {}'.format(search_row['company_name']))
            return '-'

        if not vol_unit:
            Log.warn('大株主株式数単位不正: {} dec_date, vol_unit: {}'.format(
                search_row['company_name'], t['dec_date']))
            return '-'

        dec = self.convert_date(t['dec_date'])
        unit = self.convert_unit(vol_unit)
        return {'holders': holders, 'date': dec, 'unit': unit}

    def convert_date(self, str_date):
        str_date = self.norm(str_date)
        if '平成' in str_date:
            jap = DateTimeUtil.date_from_japanese_era(str_date)
            if jap:
                return DateTimeUtil.strf_ymd_st(jap)
            else:
                return '-'
        else:
            m = re.search('(\d+)年(\d+)月(\d+)日', str_date)
            if m:
                return DateTimeUtil.strf_ymd_st(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            else:
                return '-'

    def convert_unit(self, str_vol_unit):
        if '十' in str_vol_unit:
            return "10"
        if '百' in str_vol_unit:
            return "100"
        if '千' in str_vol_unit:
            return "1000"
        if '万' in str_vol_unit:
            return "10000"
        else:
            return "1"

    def norm(self, text, blank_cut=True):
        ret = zenhan.z2h(text, 3)
        if blank_cut:
            ret = ret.replace(' ', '')
        return ret

    def tag_remove(self, text):
        return re.sub(r'<.+?>', '', text)

    def find_text(self, target, search_text):
        return target.find(search_text) >= 0

    def company_name_normarize(self, company_name):
        name = company_name.replace('(株)', '').replace(' ', '').replace('－', '−')
        name = re.sub(r'^　', '', name)
        name = re.sub(r'　$', '', name)
        return name
