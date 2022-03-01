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


# freestock = pd.read_excel('C:/Users/ysj/Desktop/freestock.xlsx')
ipt = pd.read_excel('C:/Users/ysj/Desktop/freerates.xlsx')

def get_bdate_info(start_date, end_date) :
    end_date = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") - relativedelta(days=1),"%Y%m%d"))
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_date)).rename(columns={0: '일자'})
    prevbdate = date.shift(1).rename(columns={'일자': '전영업일자'})
    date = pd.concat([date, prevbdate], axis=1).fillna(
        datetime.strftime(datetime.strptime(stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(start_date, "%Y%m%d") - relativedelta(days=1), "%Y%m%d")), "%Y%m%d"),"%Y-%m-%d %H:%M:%S"))
    date['마지막영업일'] = ''
    for i in range(0, len(date) - 1):
        if abs(datetime.strptime(datetime.strftime(date.iloc[i + 1].일자, "%Y%m%d"), "%Y%m%d") - datetime.strptime(datetime.strftime(date.iloc[i].일자, "%Y%m%d"), "%Y%m%d")).days > 1:
            date['마지막영업일'].iloc[i] = 1
        else:
            date['마지막영업일'].iloc[i] = 0
    return date

def get_df_input(ipt) :
    df = pd.DataFrame()
    for i in range(0,len(ipt)):
        df_temp = pd.DataFrame({'stddate': list(np.repeat(datetime.strftime(ipt['Symbol'].iloc[i], '%Y-%m-%d'),len(ipt.columns) - 1)),
                                'code' : list(ipt.columns[1:len(ipt.columns)]) ,
                                'freerate' : list(ipt[ipt['Symbol']==datetime.strftime(ipt['Symbol'].iloc[i], '%Y-%m-%d')].iloc[0][1:len(ipt.columns)])})
        df = pd.concat([df, df_temp], axis = 0).fillna(0).reset_index(drop=True)
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

start_date = '20220224'
end_date = '20220228'
def get_stock_foreign(start_date, end_date) :
    bdate = get_bdate_info(start_date, end_date)
    df = pd.DataFrame()
    for i in range(0,len(bdate)):
        df_temp = stock.get_exhaustion_rates_of_foreign_investment_by_ticker(datetime.strftime(bdate.iloc[i].일자, "%Y%m%d"), market = "ALL").reset_index()
        sector = get_sector(datetime.strftime(bdate.iloc[i].일자, "%Y%m%d"))[['티커','종목명','섹터','업종']]
        df_temp = pd.merge(df_temp, sector, on = '티커', how = 'inner')
        df_temp['종목코드'] = 'A' + df_temp['티커']
        df_temp['날짜'] = datetime.strftime(bdate.iloc[i].일자, "%Y-%m-%d")
        df_temp = df_temp[['날짜','티커','종목코드','종목명','섹터','업종','상장주식수','보유수량','지분율']]
        df_temp.columns= ['stddate','ticker','code','company','sector','industry','tot_qty','frn_qty','frn_rate']
        df = pd.concat([df, df_temp], axis=0)
    return df

df = get_stock_foreign(start_date, end_date)
df_free = get_df_input(ipt)