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
    trend_strength_df = pd.DataFrame(columns=['trend_strength'])
    for k, g in grouped:
        g = g.sort_values(by='bar_time', ascending=True)
        trend_strength = (g.close_price.iloc[-1]-g.close_price.iloc[0])/g.close_price.diff().fillna(0).map(np.abs).sum()
        trend_strength_df.loc[k, :] = trend_strength
    trend_strength_df = trend_strength_df
    trend_strength_df.index.name = 'code'
    trend_strength_df['trade_date'] = trade_date
    return trend_strength_df

def calc_factor_by_code(params):
    k = params[0]
    g = params[1]
    n_windows = params[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g['trend_strength'] = g['trend_strength'].rolling(window=n_windows).mean().shift(1)
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
        trend_strength_res = p.map(calc_factor_by_code, grouped_list)

    trend_strength_df = pd.concat(trend_strength_res).reset_index(drop=True)
    trend_strength_df = trend_strength_df.dropna()
    return trend_strength_df.loc[:, ["trade_date", "code", "trend_strength"]]

if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)
    print(calc_factor(begin_date,end_date))