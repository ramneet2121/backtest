import numpy as np
import pandas as pd
import datetime
import psycopg2


class PythonBacktestUtils:

    def __init__(self, quantity, underlying):
        self.quantity = quantity
        self.underlying = underlying

    def connect_database(self):
        self.conn = psycopg2.connect(
            database="database", user='postgres', password='Ram@2001', host='localhost', port='5432'
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def collect_data(self, table, start_date, end_date):
        result = pd.read_sql_query(f'''SELECT * from "{table}" where "{table}"."Date" >= {start_date} and 
                    "{table}"."Date" <= {end_date};''', self.conn)
        return result

    def calculate_strike(self, series):
        strike_list = list(np.sort(series))
        return strike_list

    def first_exp(self, series, current_date):
        count = 0
        all_exp = list(series)
        all_exp.sort()
        first_exp_date = all_exp[count]
        while first_exp_date < current_date:
            count += 1
            first_exp_date = all_exp[count]
        return first_exp_date

    def next_exp(self, series, current_date):
        count = 0
        all_exp = list(series.unique())
        all_exp.sort()
        first_exp_date = all_exp[count]
        second_exp_date = all_exp[count + 1]
        while first_exp_date < current_date:
            count += 1
            first_exp_date = all_exp[count]
            second_exp_date = all_exp[count + 1]
        return second_exp_date

    def closest_value(self, lst, k):
        return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - k))]

    def change_list_to_float(self, lst):
        new_list = []
        for x in lst:
            new_list.append(float(x))
        return new_list

    def convert_date_to_str(self, date):
        date_ = str(date)
        return date_[:4] + '-' + date_[4:6] + '-' + date_[6:8]
        # return date_[6:8] + '-' + date_[4:6] + '-' + date_[:4]
    
    def convert_date_to_int(self, date):
        return int(date[:4] + date[5:7] + date[8:10])

    def convert_secs_to_timestamp(self, secs):
        return datetime.time(secs // 3600, (secs % 3600) // 60, secs % 60)

    def convert_timestamp_to_secs(self, timeval):
        return timeval.hour * 3600 + timeval.minute * 60 + timeval.second

    def change_time(self, dataframe):
        dataframe['Time'] = pd.to_datetime(dataframe['Time'], unit='s').dt.strftime('%H:%M:%S')
        dataframe['Time'] = dataframe['Time'].apply(lambda x: datetime.datetime.strptime(x, '%H:%M:%S'))
        dataframe['Time'] = dataframe['Time'].apply(lambda x: x.time())
        return dataframe

    def add_secs_to_time(self, timeval, secs_to_add):
        secs = self.convert_timestamp_to_secs(timeval)
        secs += secs_to_add
        return datetime.time(secs // 3600, (secs % 3600) // 60, secs % 60)

    def closest_premium(self, df, time_, value):
        strike_list = list(df['Strike'].unique())
        strike_list.sort(reverse=True)
        price_list = []
        new_strike_list = []

        for strike in strike_list:

            if df[(df['Strike'] == strike) & (df['Time'] == time_)].empty:
                continue

            price = df[(df['Strike'] == strike) & (df['Time'] == time_)].Close.max()
            price_list.append(price)
            new_strike_list.append(strike)

        current_price = self.closest_value(price_list, value)
        current_strike = new_strike_list[price_list.index(current_price)]

        return current_price, current_strike

    def default_entry_fill(self, date_, instrument, type_, entry_time, exit_time, entry_spot, exit_spot, exit_price,
                           entry_price):
        entry_dict = {
            'date': self.convert_date_to_str(date_),
            'underlying': self.underlying,
            'instrument': instrument,
            'type': type_,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'entry_spot': entry_spot,
            'exit_spot': exit_spot,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl': (entry_price - exit_price) * self.quantity
        }
        return entry_dict
