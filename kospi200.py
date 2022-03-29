import datetime
import pandas as pd
from pykrx import stock
import requests
from datetime import datetime
from io import BytesIO
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings( 'ignore' )
import numpy as np


# kospi 구성종목 크롤링
# 0. 영업일 가져오기
# 1. 코스피200 구성종목 출력
# 2. 펀더멘털 계산


# 0. 영업일 가져오기
def get_bdate_info(start_date, end_date) :
    end_date = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") - relativedelta(days=1),"%Y%m%d"))
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_date)).rename(columns={0: '일자'})
    prevbdate = date.shift(1).rename(columns={'일자': '전영업일자'})
    date = pd.concat([date, prevbdate], axis=1).fillna(
        datetime.strftime(datetime.strptime(stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(start_date, "%Y%m%d") - relativedelta(days=1), "%Y%m%d")), "%Y%m%d"),"%Y-%m-%d %H:%M:%S"))
    date['주말'] = ''
    for i in range(0, len(date) - 1):
        if abs(datetime.strptime(datetime.strftime(date.iloc[i + 1].일자, "%Y%m%d"), "%Y%m%d") - datetime.strptime(datetime.strftime(date.iloc[i].일자, "%Y%m%d"), "%Y%m%d")).days > 1:
            date['주말'].iloc[i] = 1
        else:
            date['주말'].iloc[i] = 0
    month_list = date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')).unique()
    monthly = pd.DataFrame()
    for m in month_list:
        try:
            monthly = monthly.append(date[date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
        except Exception as e:
            print("Error : ", str(e))
        pass
    date['월말'] = np.where(date['일자'].isin(monthly.일자.tolist()), 1, 0)
    return date


start_date = '20170101'
end_date = '20220228'
bdate = get_bdate_info(start_date, end_date)


# 1. 코스피200 구성종목 출력
def get_k200_pdf(stddate):
    query_str_parms = {
    'locale': 'ko_KR',
    'tboxindIdx_finder_equidx0_0': '코스피 200',
    'indIdx': '1',
    'indIdx2': '028',
    'codeNmindIdx_finder_equidx0_0': '코스피 200',
    'param1indIdx_finder_equidx0_0': '',
    'trdDd': stddate,
    'money': '3',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
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
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df['날짜'] = datetime.strftime(datetime.strptime(stddate, "%Y%m%d"), "%Y-%m-%d")
    df = df[['날짜', '종목코드', '종목명', '종가', '등락률', '상장시가총액']]
    return df


pdf = pd.DataFrame()
for i in range(0,len(bdate)):
    pdf_temp = get_k200_pdf(datetime.strftime(bdate.iloc[i].일자, "%Y%m%d"))
    pdf = pd.concat([pdf,pdf_temp],axis= 0)

pdf.to_excel('C:/Users/Check/Desktop.dff.xlsx')


df_pdf = pd.DataFrame()
for i in range(0,len(pdf)):
    df_mktcap = stock.get_market_cap(bdate.iloc[i].일자,bdate.iloc[i].일자,pdf.iloc[i].종목코드)
    df_fmt  = stock.get_market_fundamental(bdate.iloc[i].일자,bdate.iloc[i].일자,pdf.iloc[i].종목코드)
    df_all = pd.merge(df_mktcap, df_fmt, left_index = True, right_index= True, how = 'left')
    df_all['종가'] = df_all['시가총액'] / df_all['상장주식수']
    df_all['순이익'] = df_all['EPS'] * df_all['상장주식수']
    df_pdf_temp = df_all
    df_pdf_temp['티커'] = pdf.iloc[i].종목코드
    df_pdf_temp['종목명'] = pdf.iloc[i].종목명
    df_pdf = pd.concat([df_pdf, df_pdf_temp])

df_pdf.to_excel('C:/Users/Check/Desktop.dff.xlsx')