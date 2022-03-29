import pandas as pd
from pykrx import stock
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO

# 0. ETF정보 가져오기
# 1. ETF PDF 가져오기
# 2. 최종 종목 정리하기


start_date = '20180511'
end_date = '20220311'


# 0. ETF 정보 가져오기
def get_etf_info_GUBUN(stddate):
    query_str_parms = {
    'locale': 'ko_KR',
    'idxMktClssId2': '',
    'inqCondTpCd1': '0',
    'inqCondTpCd3': '0',
    'inqCondTpCd4': '0',
    'inqCondTpCd2': '0',
    'srchStrNm': '',
    'idxAsstClssId1': '00',
    'idxMktClssId': '00',
    'idxMktClssId3': '01',
    'idxMktClssId1': '02',
    'countryBox2': '0208',
    'countryBox1': '',
    'idxAsstClssId2': '00',
    'idxAsstClssId3': '00',
    'taxTpCd': '0',
    'idxLvrgInvrsTpCd': 'TT',
    'asstcomId': '00000',
    'gubun': '1',
    'trdDd': stddate,
    # 'strtDd': y1_ago,
    'endDd': stddate,
    'inqCondTp1_Box1': '0',
    'inqCondTp2_Box1': '0',
    'inqCondTp3_Box1': '0',
    'inqCondTp4_Box1': '0',
    'inqCondTpCd5': '0',
    'inqCondTp1_Box2': '0',
    'inqCondTp3_Box2': '0',
    'inqCondTp4_Box2': '0',
    'inqCondTpCd6': '1',
    'sortMethdTpCd': '2',
    'inqCondTp2_Box2': '0',
    'inqCondTpCd7': '0',
    'inqCondTpCd8': '0',
    'inqCondTpCd9': '0',
    'money': '3',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT05101'
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
    df['자산분류'] = ''
    df['자산분류상세'] = ''
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
        df['자산분류'].iloc[i] = df['분류체계'][i].split('-')[0]
        try:
            df['자산분류상세'].iloc[i] = df['분류체계'][i].split('-')[1]
        except:
            df['자산분류상세'].iloc[i] = df['분류체계'][i].split('-')[0]
    df = df[['종목코드','종목명','상장일','기초지수','복제방법','총보수','자산분류','자산분류상세']].rename(columns={'종목코드':'ETF티커','종목명':'ETF명'})
    return df
def get_etf_info_CU():
    query_str_parms = {
        'locale': 'ko_KR',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04601'
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
    for i in range(0, len(df.단축코드)):
        df.단축코드.iloc[i] = str(df.단축코드[i]).zfill(6)
    df = df.rename(columns={'단축코드':'ETF티커'})
    return df
def get_etf_info(stddate,asset,market) :
    etf_info = get_etf_info_GUBUN(stddate)
    etf_info_CU = get_etf_info_CU()[['ETF티커','기초시장분류','CU수량','상장좌수']]
    etf_info = pd.merge(etf_info, etf_info_CU, on = 'ETF티커', how = 'inner' )
    etf_info = etf_info[etf_info.자산분류.isin([asset]) & etf_info.기초시장분류.isin([market]) & etf_info.자산분류상세.isin(['업종섹터', '전략', '규모'])].reset_index(drop=True)
    filter = stock.get_etf_ohlcv_by_ticker(stddate).reset_index(drop=False)[['티커','시가총액']].rename(columns={'티커': 'ETF티커'})
    etf_info = pd.merge(etf_info, filter, on = 'ETF티커', how = 'inner')
    etf_info = etf_info[etf_info.시가총액 > 10000000000].sort_values('시가총액')
    etf_info = etf_info[['ETF티커', 'ETF명', '기초시장분류', '자산분류', '자산분류상세', '복제방법', '기초지수', '상장일', 'CU수량']]
    etf_code = list(etf_info.ETF티커)
    return etf_info, etf_code

asset = '주식'
market = '국내'
etf_info = get_etf_info(end_date,asset,market)[0]
etf_code = get_etf_info(end_date,asset,market)[1]
# tgt_etf = etf_info[etf_info.ETF명.str.contains('수소|친환경')]
tgt_etf = etf_info[etf_info.ETF티커.str.contains('367770')]

# 0. ETF PDF 가져오기
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
def get_pdf_info(ticker, start_date, end_date) :
    bdate =  get_bdate_info(start_date, end_date)
    df_pdf = pd.DataFrame()
    for i in range(0,len(bdate)) :
        try :
            df_pdf_temp = stock.get_etf_portfolio_deposit_file(ticker, bdate.iloc[i].일자)
            df_pdf = pd.concat([df_pdf, df_pdf_temp])
        except :
            pass
    df_pdf = df_pdf.reset_index(drop = False)
    stock_info = get_ticker_info()
    df_stock = pd.DataFrame({'티커' : df_pdf.티커.unique(), '종목명' : df_pdf.종목명.unique()})
    df_stock = df_stock.drop(df_stock[df_stock.종목명=='원화현금'].index)
    df_stock = pd.merge(df_stock, stock_info[['티커','시장구분']], on ='티커', how ='inner')
    df_prc = stock.get_market_ohlcv_by_ticker(end_date, market="ALL")[['종가','시가총액','거래대금']]
    df_stock = pd.merge(df_stock, df_prc, on ='티커', how ='inner').sort_values('시가총액', ascending = False)
    df_stock['시가총액'] = df_stock['시가총액'] / 100000000
    df_stock['거래대금'] = df_stock['거래대금'] / 100000000
    return df_stock

ticker = '367770'
df = get_pdf_info(ticker, start_date, end_date)

df.to_excel('C:/Users/Check/Desktop/df.xlsx')