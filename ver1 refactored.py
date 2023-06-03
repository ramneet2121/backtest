import pandas as pd
import numpy as np
import datetime
import warnings
warnings.filterwarnings('ignore')
import sys
sys.path.append('C:\AlgoTest\python lib')
from PythonBacktestUtils.PythonBacktestUtils import PythonBacktestUtils

pbu = PythonBacktestUtils(25, 'BN Cash')

data_call = pd.read_csv(r"C:\AlgoTest\db\OPT\BN_CALL.csv")
data_put = pd.read_csv(r"C:\AlgoTest\db\OPT\BN_PUT.csv")
data_cash = pd.read_csv(r"C:\AlgoTest\db\OPT\BN_CASH.csv")

data_cash = data_cash[data_cash['Date'] >= 20210101]
data_call = data_call[data_call['Date'] >= 20210101]
data_put = data_put[data_put['Date'] >= 20210101]

def entry_fill(instrument, type_, entry_time, entry_price, entry_spot, strike, date_, exit_time, exit_price, exit_spot, reason, closest_premium):
    
    dict_ = pbu.default_entry_fill(date_, instrument, type_, entry_time, exit_time, entry_spot, exit_spot, exit_price, entry_price)
    
    dict_['strike'] = strike
    dict_['closest_premium'] = closest_premium
    dict_['reason'] = reason
    
    return dict_

def initial_trade(strangle, call_price, put_price, call_strike, put_strike, spot, e_time, premium):
    
    strangle ={
        'call_price': call_price,
        'entry_spot_call': spot,
        'entry_time_call': e_time,
        'call_strike': call_strike,
        'closest_premium_call': premium,
        'put_price': put_price,
        'entry_spot_put': spot,
        'entry_time_put': e_time,
        'put_strike': put_strike,
        'closest_premium_put': premium
    }
    
    return strangle


def per_day_report(data_cash, data_put, data_call):
    
    df_change = pd.DataFrame()
    uni_date = data_cash.Date.unique()
    uni_date.sort()
    
    for count, date in enumerate(uni_date):
            
        df_cash = data_cash[data_cash['Date'] == date].copy()
        df_put = data_put[data_put['Date'] == date].copy()
        df_call = data_call[data_call['Date'] == date].copy()
        
        first_exp_date = pbu.first_exp(df_put['Expiry'], date)
        
        df_put = df_put[df_put['Expiry'] == first_exp_date].copy()
        df_call = df_call[df_call['Expiry'] == first_exp_date].copy()
        
        df_cash.sort_values('Time')
        df_put.sort_values('Time')
        df_call.sort_values('Time') 
        
        df_cash.reset_index(inplace = True)
        df_put.reset_index(inplace = True)
        df_call.reset_index(inplace = True)
        
        l = strategy(df_cash, df_put, df_call, date)
        
        for x in l:
            df_change = df_change.append(x, ignore_index=True)
    
    return df_change

def strategy(df_cash, df_put, df_call, date_):
    
    a = []
    start_time = datetime.time(9, 29, 00)
    end_time = datetime.time(15, 25, 00)
    start_time_int = pbu.convert_timestamp_to_secs(start_time)
    end_time_int = pbu.convert_timestamp_to_secs(end_time)
    trade_flag = 0
    premium = 50
    total_premium = 200
    strangle = {}
    
    for idx, row in df_cash.iterrows():

        if (row['Time'] >= end_time_int):

            if trade_flag == 1:

                if df_call[ (df_call['Strike'] == strangle['call_strike']) & (df_call['Time'] == row['Time']) ].empty:
                    continue
                    
                if df_put[ (df_put['Strike'] == strangle['put_strike']) & (df_put['Time'] == row['Time']) ].empty:
                    continue

                current_call_price = df_call[(df_call['Strike'] == strangle['call_strike']) & (df_call['Time'] == row['Time'])].Close.max()                
                current_put_price = df_put[(df_put['Strike'] == strangle['put_strike']) & (df_put['Time'] == row['Time'])].Close.max()

                dict_ = entry_fill('PE', 'SELL', strangle['entry_time_put'], strangle['put_price'], strangle['entry_spot_put'], strangle['put_strike'], date_, row['Time'], current_put_price, row['Close'], 'Exit Time', strangle['closest_premium_put']) 
                a.append(dict_)

                dict_ = entry_fill('CE', 'SELL', strangle['entry_time_call'], strangle['call_price'], strangle['entry_spot_call'], strangle['call_strike'], date_, row['Time'], current_call_price, row['Close'], 'Exit Time', strangle['closest_premium_call']) 
                a.append(dict_)

                break

        if (row['Time'] > start_time_int) and (row['Time'] < end_time_int): 

            if trade_flag == 0:

                # call closest premium
                current_call_price, current_call_strike = pbu.closest_premium(df_call, row['Time'], premium) 

                # put closest premium
                current_put_price, current_put_strike = pbu.closest_premium(df_put, row['Time'], premium)
                
                # initial trade
                strangle = initial_trade(strangle, current_call_price, current_put_price, current_call_strike, current_put_strike, row['Close'], row['Time'], premium)
                trade_flag = 1
                continue

            if trade_flag == 1:

                if df_call[ (df_call['Strike'] == strangle['call_strike']) & (df_call['Time'] == row['Time']) ].empty:
                    continue
                    
                if df_put[ (df_put['Strike'] == strangle['put_strike']) & (df_put['Time'] == row['Time']) ].empty:
                    continue

                current_call_price = df_call[(df_call['Strike'] == strangle['call_strike']) & (df_call['Time'] == row['Time'])].Close.max()                
                current_put_price = df_put[(df_put['Strike'] == strangle['put_strike']) & (df_put['Time'] == row['Time'])].Close.max()

                if ((current_call_price + current_put_price) >= total_premium):

                    dict_ = entry_fill('PE', 'SELL', strangle['entry_time_put'], strangle['put_price'], strangle['entry_spot_put'], strangle['put_strike'], date_, row['Time'], current_put_price, row['Close'], 'Total Premium Hit', strangle['closest_premium_put'] ) 
                    a.append(dict_)

                    dict_ = entry_fill('CE', 'SELL', strangle['entry_time_call'], strangle['call_price'], strangle['entry_spot_call'], strangle['call_strike'], date_, row['Time'], current_call_price, row['Close'], 'Total Premium Hit', strangle['closest_premium_call']) 
                    a.append(dict_)

                    new_call_price, new_call_strike = pbu.closest_premium(df_call, row['Time'], premium) 

                    new_put_price, new_put_strike = pbu.closest_premium(df_put, row['Time'], premium)
                    
                    strangle = {}
                    strangle = initial_trade(strangle, new_call_price, new_put_price, new_call_strike, new_put_strike, row['Close'], row['Time'], premium)
                    continue

                if (current_put_price <= 0.5*current_call_price):

                    dict_ = entry_fill('PE', 'SELL', strangle['entry_time_put'], strangle['put_price'], strangle['entry_spot_put'], strangle['put_strike'], date_, row['Time'], current_put_price, row['Close'],  'PE half of CE', strangle['closest_premium_put']) 
                    a.append(dict_)

                    new_put_price, new_put_strike = pbu.closest_premium(df_put, row['Time'], current_call_price)
                    strangle['put_price'] = new_put_price
                    strangle['entry_spot_put'] = row['Close']
                    strangle['entry_time_put'] = row['Time']
                    strangle['put_strike'] = new_put_strike
                    strangle['closest_premium_put'] = current_call_price
                    continue

                if (current_call_price <= 0.5*current_put_price):
                    
                    dict_ = entry_fill('CE', 'SELL', strangle['entry_time_call'], strangle['call_price'], strangle['entry_spot_call'], strangle['call_strike'], date_, row['Time'], current_call_price, row['Close'], 'CE half of PE', strangle['closest_premium_call']) 
                    a.append(dict_)

                    new_call_price, new_call_strike = pbu.closest_premium(df_call, row['Time'], current_put_price)
                    strangle['call_price'] = new_call_price
                    strangle['entry_spot_call'] = row['Close']
                    strangle['entry_time_call'] = row['Time']
                    strangle['call_strike'] = new_call_strike
                    strangle['closest_premium_call'] = current_put_price
                    continue
          
    return a


df_final = per_day_report(data_cash, data_put, data_call)

df_final.to_csv('tradefile_after_refactor.csv')



