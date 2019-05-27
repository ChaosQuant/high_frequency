# coding=utf-8

import pdb
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from PyFin.api import DateUtilities
from models import Market5MinBar,Market

def calc_factor_by_day(trade_date: datetime.datetime) -> pd.DataFrame:
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB)
    query = select([table.trade_date,table.code,table.bar_time,table.close_price,
                    ]).where(and_(table.trade_date==trade_date))
    
    df = pd.read_sql(query,conn)
    grouped = df.groupby('code')
    improved_reversal_df = pd.DataFrame(columns=['improved_reversal'])
    for k, g in grouped:
        g = g.sort_values(by='bar_time', ascending=True)
        improved_reversal = (g.close_price.iloc[-1]-g.close_price.iloc[6])/g.close_price.iloc[6]
        improved_reversal_df.loc[k, :] = improved_reversal
    improved_reversal_df = improved_reversal_df
    improved_reversal_df.index.name = 'code'
    improved_reversal_df['trade_date'] = trade_date
    return improved_reversal_df

def calc_factor_by_code(params):
    k = params[0]
    g = params[1]
    n_windows = params[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g['improved_reversal'] = g['improved_reversal'].rolling(window=n_windows).mean().shift(1)
    g['code'] = k
    return g

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime) -> pd.DataFrame:
    n_windows = 4
    trade_date_list = DateUtilities.makeSchedule(begin_date,end_date,'1b','china.sse')
    
    res = []
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        res = p.map(calc_factor_by_day, trade_date_list)
    res = pd.concat(res).reset_index()

    grouped = res.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        improved_reversal_res = p.map(calc_factor_by_code, grouped_list)
        
    improved_reversal_df = pd.concat(improved_reversal_res).reset_index(drop=True)
    improved_reversal_df = improved_reversal_df.dropna()
    return improved_reversal_df.loc[:, ["trade_date", "code", "improved_reversal"]]

if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)
    print(calc_factor(begin_date,end_date))