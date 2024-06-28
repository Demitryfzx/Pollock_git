import pandas as pd
import numpy as np
# Note that the import is package datetime, but not module datetime
import datetime
import pytz
import concurrent.futures

from sync_session import SyncSession

TICKERS = ['TAREDSC',
 'FDTR',
 'RBATCTR',
 'BZSTSETA',
 'NOBRDEPA',
 'SZLTDEP',
 'UKBRBASE',
 'PPCBON',
 'SWRRATEI',
 'TUBR1WRA',
 'ISBRANN',
 'NZOCR',
 'MAOPRATE',
 'KORP7DR',
 'SARPRT',
 'CABROVER',
 'RREFKANN',
 'INRPYLDP',
 'CHLR12M',
 'DEBRDISC',
 'EGBRDR',
 'HKBASE',
 'ICBRANN',
 'QAIRONLR',
 'SRREPO',
 'CUAEBASE',
 'VNREFINC']

COUNTRY_TO_CENTRALBANK = {
    'Taiwan': 'CBC',  # Central Bank of the Republic of China (Taiwan)
    'United States': 'FED',  # Federal Reserve
    'Australia': 'RBA',  # Reserve Bank of Australia
    'Brazil': 'BCB',  # Banco Central do Brasil
    'Norway': 'Norges Bank',  # Norges Bank
    'Switzerland': 'SNB',  # Swiss National Bank
    'United Kingdom': 'BOE',  # Bank of England
    'Philippines': 'BSP',  # Bangko Sentral ng Pilipinas
    'Sweden': 'Riksbank',  # Sveriges Riksbank
    'Turkey': 'CBRT',  # Central Bank of the Republic of Turkey
    'Israel': 'BOI',  # Bank of Israel
    'New Zealand': 'RBNZ',  # Reserve Bank of New Zealand
    'Malaysia': 'BNM',  # Bank Negara Malaysia
    'South Korea': 'BOK',  # Bank of Korea
    'South Africa': 'SARB',  # South African Reserve Bank
    'Canada': 'BOC',  # Bank of Canada
    'Russia': 'CBR',  # Central Bank of Russia
    'India': 'RBI',  # Reserve Bank of India
    'China': 'PBOC',  # People's Bank of China
    'Denmark': 'DN',  # Danmarks Nationalbank
    'Egypt': 'CBE',  # Central Bank of Egypt
    'Hong Kong': 'HKMA',  # Hong Kong Monetary Authority
    'Iceland': 'CBI',  # Central Bank of Iceland
    'Qatar': 'QCB',  # Qatar Central Bank
    'Saudi Arabia': 'SAMA',  # Saudi Arabian Monetary Authority
    'UAE': 'CBUAE',  # Central Bank of the United Arab Emirates
    'Vietnam': 'SBV'  # State Bank of Vietnam
}

TICKER_TO_COUNTRY = {
    'TAREDSC': 'Taiwan',
    'FDTR': 'United States',
    'RBATCTR': 'Australia',
    'BZSTSETA': 'Brazil',
    'NOBRDEPA': 'Norway',
    'SZLTDEP': 'Switzerland',
    'UKBRBASE': 'United Kingdom',
    'PPCBON': 'Philippines',
    'SWRRATEI': 'Sweden',
    'TUBR1WRA': 'Turkey',
    'ISBRANN': 'Israel',
    'NZOCR': 'New Zealand',
    'MAOPRATE': 'Malaysia',
    'KORP7DR': 'South Korea',
    'SARPRT': 'South Africa',
    'CABROVER': 'Canada',
    'RREFKANN': 'Russia',
    'INRPYLDP': 'India',
    'CHLR12M': 'China',
    'DEBRDISC': 'Denmark',
    'EGBRDR': 'Egypt',
    'HKBASE': 'Hong Kong',
    'ICBRANN': 'Iceland',
    'QAIRONLR': 'Qatar',
    'SRREPO': 'Saudi Arabia',
    'CUAEBASE': 'UAE',
    'VNREFINC': 'Vietnam'
}

COUNTRY_TO_RELEASETIME = {
    'Taiwan': (16, 30),
    'United States': (14, 0),
    'Australia': (14, 30),
    'Brazil': (18, 30),
    'Norway': (10, 0),
    'Switzerland': (9, 30),
    'United Kingdom': (12, 0),
    'Philippines': (15, 0),
    'Sweden': (9, 30),
    'Turkey': (14, 0),
    'Israel': (18, 0),
    'New Zealand': (14, 0),
    'Malaysia': (15, 0),
    'South Korea': (10, 0),
    'South Africa': (15, 0),
    'Canada': (10, 0),
    'Russia': (13, 30),
    'India': (10, 0),
    'Hong Kong': (8, 0)
}

def ticker_to_index(ticker):
    return f"{ticker} Index"

# Timezone Conversion
def country_name_standardization(name: str):
    '''
    This function is to only stardize the three countries name so that it can fit the country names in pytz package
    '''
    if (name == "UAE"):
        return "United Arab Emirates"
    elif (name == "South Korea"):
        return "Korea (South)"
    elif (name == "United Kingdom"):
        return "Britain (UK)"
    else:
        return name

def obtain_timezone(index_name):
    '''
    This function is to obtain the timezone of the given index
    '''
    country_name = TICKER_TO_COUNTRY[index_name[:-6]]
    name_stand = country_name_standardization(country_name)
    for k, v in dict(pytz.country_names).items():
        if v == name_stand:
            code = k
            return pytz.country_timezones[code][0]
    
    raise Exception("Country name doesn't exist")

def retrive_single_futureversion(index_name: str, 
                   checker = False):
    '''
    The function is to retrieve the information of the next policy rate release date
    checker: Default is False, it will print the res dataframe if it is set to True, 
    and then return the res dataframe
    '''
    def handler(a,b):
        pass
    session = SyncSession(handler)
    REFDATA_SVC = '//blp/refdata'
    session.openService(REFDATA_SVC)
    refdata_svc = session.getService(REFDATA_SVC)

    # We are not looking for Historical Data, but data in the future
    rqst = refdata_svc.createRequest(
        'ReferenceDataRequest')

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    end_date = (datetime.datetime.now() + datetime.timedelta(days = 365)).strftime("%Y%m%d")

    try:
        rqst.fromPy({
            'securities': [index_name],
            'fields': ['ECO_FUTURE_RELEASE_DATE_LIST'],
            'overrides': [
            {'fieldId': 'START_DT', 'value': current_date},
            {'fieldId': 'END_DT', 'value': end_date},
        ]
        })
        data = session.sendRequest([{'request': rqst}])

        for event in data:
            for message in event:
                item = message.toPy()['securityData']
                for x in item:
                    ticker = x['security']
                    res=pd.DataFrame(x['fieldData']['ECO_FUTURE_RELEASE_DATE_LIST'])
                    res['ticker']=ticker
        
        if checker:
            print(res)
            return res
        
        next_release = res[res.index == 0]['Release Dates & Times'][0]
        next_release = datetime.datetime.strptime(next_release, '%Y/%m/%d %H:%M:%S')
        
        return next_release
    
    except:
        print("This Ticker doesn't provide meeting dates")
        return None

def obtain_single_future_output(ticker):
    index_name = ticker_to_index(ticker)
    release_date = retrive_single_futureversion(index_name, checker = False)
    timezone = obtain_timezone(index_name)
    country_name = TICKER_TO_COUNTRY[ticker]

    bankname = COUNTRY_TO_CENTRALBANK[country_name]
    
    return {"date": release_date, 
            "country": country_name, 
            "description": f"{bankname} rate release", 
            "timezone": timezone}

def obtain_all_future_output():
    future_dates = dict()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(obtain_single_future_output, ticker): 
                   ticker for ticker in TICKERS}
        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                future_dates[ticker] = result
            except Exception as exc:
                print(f'{ticker} generated an exception: {exc}')
    
    return future_dates

def retrive_single_historicalversion(index_name: str, 
                   checker = False):
    '''
    The function is to retrieve the information of the historical policy rate release date
    checker: Default is False, it will print the res dataframe if it is set to True, 
    and then return the res dataframe
    '''
    def handler(a,b):
        pass
    session = SyncSession(handler)
    REFDATA_SVC = '//blp/refdata'
    session.openService(REFDATA_SVC)
    refdata_svc = session.getService(REFDATA_SVC)

    # We are not looking for Historical Data, but data in the future
    rqst = refdata_svc.createRequest(
        'HistoricalDataRequest')

    previous_date = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime("%Y%m%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days = 365 * 4)).strftime("%Y%m%d")

    try:
        rqst.fromPy({
            'securities': [index_name],
            'fields': ['ECO_RELEASE_DT'],
            'startDate': start_date,
            'endDate': previous_date,
            'periodicitySelection': "DAILY"
        })
        data = session.sendRequest([{'request': rqst}]) 
            
        for event in data:   
            for message in event:
                item= message.toPy()
                ticker = item['securityData']['security']
                res = pd.DataFrame(item['securityData']['fieldData'])
                res['ticker']=ticker
        
        if checker:
            print(res)
            return res
        
        historical_release = res['date'].tolist()
        return historical_release
    
    except:
        print("This Ticker doesn't provide meeting dates")
        return None

def obtain_single_historical_output(ticker):
    index_name = ticker_to_index(ticker)
    release_date = retrive_single_historicalversion(index_name, checker = False)
    timezone = obtain_timezone(index_name)
    country_name = TICKER_TO_COUNTRY[ticker]

    bankname = COUNTRY_TO_CENTRALBANK[country_name]

    to_return = []
    for d in release_date:
        to_return.append({"date": d, 
                            "country": country_name, 
                            "description": f"{bankname} rate release", 
                            "timezone": timezone})
    
    return to_return

def obtain_all_historical_output():
    historical_dates = dict()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        histories = {executor.submit(obtain_single_historical_output, ticker): 
                   ticker for ticker in TICKERS}
        for history in concurrent.futures.as_completed(histories):
            ticker = histories[history]
            try:
                result = history.result()
                historical_dates[ticker] = result
            except Exception as exc:
                print(f'{ticker} generated an exception: {exc}')
    
    return historical_dates


if __name__ == '__main__':
    obtain_all_future_output()

    print("----------------------------------------------------------------")

    obtain_all_historical_output()