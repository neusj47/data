import datetime
import pymysql
from time import sleep
import math
import pymysql
import pandas as pd
import yfinance as yf
from pykrx import stock
import requests
from datetime import datetime, timedelta
from io import BytesIO
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
from bs4 import BeautifulSoup
warnings.filterwarnings( 'ignore' )




conn = pymysql.connect(host='localhost', user='root', password='sjyoo1~', port = 3307, db='testdb', charset='utf8')
cursor = conn.cursor()
sql = """
CREATE TABLE IF NOT EXISTS stock_prcs (
stddate date,
code VARCHAR(20),
company VARCHAR(50),
sector VARCHAR(50),
industry VARCHAR(50),
gubun VARCHAR(50),
sosok VARCHAR(50),
prev_prc BIGINT(30),
prc BIGINT(30),
rtn FLOAT,
prev_mcap FLOAT,
mcap FLOAT,
prev_qty BIGINT(30),
qty BIGINT(30),
prev_amt FLOAT,
amt FLOAT,
PRIMARY KEY (stddate, code)
)
"""
cursor.execute(sql)
conn.commit()


stddate = '20220225'


def get_ticker_info():
    query_str_parms = {
    'locale': 'ko_KR',
    'mktId': 'ALL',
    'share': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
        }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
        }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
        }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content)).rename(columns={'단축코드':'티커'})
    return df

def get_sector(stddate):
    sector = {1010: '에너지',
              1510: '소재',
              2010: '자본재',
              2020: '상업서비스와공급품',
              2030: '운송',
              2510: '자동차와부품',
              2520: '내구소비재와의류',
              2530: '호텔,레스토랑,레저 등',
              2550: '소매(유통)',
              2560: '교육서비스',
              3010: '식품과기본식료품소매',
              3020: '식품,음료,담배',
              3030: '가정용품과개인용품',
              3510: '건강관리장비와서비스',
              3520: '제약과생물공학',
              4010: '은행',
              4020: '증권',
              4030: '다각화된금융',
              4040: '보험',
              4050: '부동산',
              4510: '소프트웨어와서비스',
              4520: '기술하드웨어와장비',
              4530: '반도체와반도체장비',
              4535: '전자와 전기제품',
              4540: '디스플레이',
              5010: '전기통신서비스',
              5020: '미디어와엔터테인먼트',
              5510: '유틸리티'}
    df = pd.DataFrame(columns=['티커', '종목명', '섹터', '업종', 'mktval', 'wgt'])
    for i, sec_code in enumerate(sector.keys()):
        response = requests.get('http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&''dt=' + stddate + '&sec_cd=G' + str(sec_code))
        if (response.status_code == 200):
            json_list = response.json()
            for json in json_list['list']:
                티커 = json['CMP_CD']
                종목명 = json['CMP_KOR']
                섹터 = json['SEC_NM_KOR']
                업종 = json['IDX_NM_KOR'][5:]
                mktval = json['MKT_VAL']
                wgt = json['WGT']
                df = df.append(
                    {'티커': 티커, '종목명': 종목명, '섹터': 섹터, '업종': 업종, 'mktval': mktval,'wgt': wgt}, ignore_index=True)
    return df

def get_adj_price(ticker) :
    response = requests.get('https://fchart.stock.naver.com/sise.nhn?symbol={}&timeframe=day&count=1000&requestType=0'.format(ticker))
    bs = BeautifulSoup(response.content, "html.parser")
    df_temp = bs.select('item')
    columns = ['Date', 'Open' ,'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame([], columns = columns, index = range(len(df_temp)))
    for i in range(len(df_temp)):
        df.iloc[i] = str(df_temp[i]['data']).split('|')
        df['Date'].iloc[i] = datetime.strftime(datetime.strptime(df['Date'].iloc[i], "%Y%m%d"), "%Y-%m-%d")
    return df

def get_daily_prc(stddate) :
    prevbdate = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
    ticker_info = get_ticker_info()[['티커','한글 종목명','시장구분','소속부']]
    ticker_info = pd.merge(ticker_info, get_sector(stddate)[['티커','종목명','섹터','업종']], on = '티커', how = 'inner')
    ticker_info['날짜'] = datetime.strftime(datetime.strptime(stddate, "%Y%m%d"), "%Y-%m-%d")
    ticker_info['수정종가'] = ''
    ticker_info['전일수정종가'] = ''
    for i in range(0, 5) :
        try :
            prc = get_adj_price(ticker_info['티커'].iloc[i])
            ticker_info['수정종가'].iloc[i] = int(prc[prc.Date == datetime.strftime(datetime.strptime(stddate, "%Y%m%d"), "%Y-%m-%d")]['Close'])
            ticker_info['전일수정종가'].iloc[i] = int(prc[prc.Date == datetime.strftime(datetime.strptime(prevbdate, "%Y%m%d"), "%Y-%m-%d")]['Close'])
        except : pass
    ticker_info['일수익률'] = ticker_info['수정종가'] / ticker_info['전일수정종가'] - 1
    mcap = stock.get_market_cap_by_ticker(datetime.strftime(datetime.strptime(stddate, "%Y%m%d"), "%Y-%m-%d"), market="ALL").reset_index(drop=False)[['티커','시가총액','상장주식수','거래대금']]
    prevmcap = stock.get_market_cap_by_ticker(datetime.strftime(datetime.strptime(prevbdate, "%Y%m%d"), "%Y-%m-%d"), market="ALL").reset_index(drop=False)[['티커','시가총액','상장주식수','거래대금']].rename(columns={'시가총액':'전일시가총액','상장주식수':'전일상장주식수','거래대금':'전일거래대금'})
    prc = pd.merge(ticker_info, mcap, on = '티커', how = 'inner')
    prc = pd.merge(prc, prevmcap, on = '티커', how = 'inner')
    prc = prc[['날짜','티커','종목명','섹터','업종','시장구분','소속부','전일수정종가','수정종가','일수익률','전일시가총액','시가총액','전일상장주식수','상장주식수','전일거래대금','거래대금']].fillna()
    prc['시가총액'] = prc['시가총액'] / 100000000
    prc['전일시가총액'] = prc['전일시가총액'] / 100000000
    prc['거래대금'] = prc['거래대금'] / 100000000
    prc['전일거래대금'] = prc['전일거래대금'] / 100000000
    return prc

prc = get_daily_prc(stddate)


for row in prc.itertuples():
    sql = """
    insert into stock_prcs
    (stddate, code, company, sector, industry, gubun, sosok, prev_prc, prc, rtn, prev_mcap, mcap, prev_qty, qty, prev_amt, amt)
     values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
                         , row[12],row[13], row[14], row[15], row[16]))
    conn.commit()



