import pandas as pd
from pykrx import stock
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO

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

stddate = '20220228'

def get_etf_info(stddate,asset,market) :
    etf_info = get_etf_info_GUBUN(stddate)
    etf_info_CU = get_etf_info_CU()[['ETF티커','기초시장분류','CU수량','상장좌수']]
    etf_info = pd.merge(etf_info, etf_info_CU, on = 'ETF티커', how = 'inner' )
    etf_info = etf_info[etf_info.자산분류.isin([asset]) & etf_info.기초시장분류.isin([market]) & etf_info.자산분류상세.isin(['업종섹터', '전략', '규모'])].reset_index(drop=True)
    etf_code = list(etf_info[etf_info.자산분류.isin([asset]) & etf_info.기초시장분류.isin([market]) & etf_info.자산분류상세.isin(['업종섹터', '전략', '규모'])].ETF티커)
    etf_info = etf_info[['ETF티커','ETF명','기초시장분류','자산분류','자산분류상세','복제방법','기초지수','상장일','CU수량']]
    etf_info.columns = [['etfcode','etfname','mkt','asset','style','gubun','idx','publicdate','cu']]
    return etf_info, etf_code

etf_info = get_etf_info(stddate,'주식','국내')[0]
etf_code = get_etf_info(stddate,'주식','국내')[1]

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

def get_etf_num(etf_code,start_date,end_date) :
    bdate = get_bdate_info(start_date, end_date)
    df = stock.get_etf_ohlcv_by_ticker(bdate.iloc[0].일자).reset_index()[['티커','상장좌수']].rename(columns={'상장좌수':bdate.iloc[0].일자})
    df = df[df['티커'].isin(etf_code)].reset_index(drop=True)
    for i in range(1,len(bdate)):
        df_temp = stock.get_etf_ohlcv_by_ticker(bdate.iloc[i].일자).reset_index()[['티커','상장좌수']].rename(columns={'상장좌수':bdate.iloc[i].일자})
        df_temp = df_temp[df_temp['티커'].isin(etf_code)].reset_index(drop=True)
        df = pd.merge(df, df_temp, on ='티커', how = 'outer')
    df_T = df.T
    df_T.columns = list(df['티커'])
    return df_T

def get_etf_prc(start_date, end_date) :
    bdate = get_bdate_info(start_date, end_date)
    df = pd.DataFrame()
    for i in range(0,len(bdate)) :
        df_temp = stock.get_etf_ohlcv_by_ticker(bdate.iloc[i].일자).reset_index(drop=False)[['티커','종가','NAV','시가총액','거래대금','상장좌수']]
        etf_info = get_etf_info(bdate.iloc[i].일자, '주식', '국내')[0][['etfcode', 'etfname', 'mkt', 'asset', 'style']].rename(columns={'etfcode': '티커'})
        df_temp = pd.merge(df_temp, etf_info, on = '티커', how ='inner')
        df_temp['날짜'] = datetime.strftime(bdate.iloc[i].일자, "%Y-%m-%d")
        df_temp = df_temp[['날짜','티커','종가','상장좌수','시가총액','NAV','거래대금']]
        df_temp['시가총액'] = df_temp['시가총액'] / 100000000
        df_temp['거래대금'] = df_temp['거래대금'] / 100000000
        df = pd.concat([df, df_temp], axis=0)
    df = [['티커','etfname','mkt','asset','style','종가','NAV','시가총액','거래대금','상장좌수']]
    df.columns = [['etfcode','etfname','mkt','asset','style','prc','nav','mcap','volume','qty']]
    return df

