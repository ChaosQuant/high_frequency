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

def calc_med_factor_by_day(trade_date: datetime.datetime) -> pd.DataFrame:
    ## bar value
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB)
    query = select([table.trade_date,table.code,table.bar_time,table.close_price,
                    table.total_volume,table.total_value]).where(and_(table.trade_date==trade_date))
    df = pd.read_sql(query,conn)
    
    grouped = df.groupby('code')
    flow_in_df = pd.DataFrame(columns=['flow_in'])
    for k, g in grouped:
        g = g.sort_values(by='bar_time', ascending=True)
        close_diff = (g.close_price - g.close_price.shift(1)).fillna(0)
        close_diff_sign = close_diff.map(lambda x: np.sign(x))
        flow_in = (g.close_price*g.total_volume*close_diff_sign).sum()
#         flow_in = (g.close_price*g.total_value*close_diff).sum() # another method for inflow
        flow_in_df.loc[k, :] = flow_in
    flow_in_df = flow_in_df.fillna(0)
    flow_in_df.index.name = 'code'
    
    ## daily value
    table2 = Market
    query2 = select([table2.trade_date,table2.code,
                    table2.turnoverValue]).where(and_(table2.trade_date==trade_date))
    df2 = pd.read_sql(query2,conn)
    
    df2.set_index("code", inplace=True)
    flow_in_df = flow_in_df.merge(df2, how="inner", left_index=True, right_index=True)
    return flow_in_df

def calc_factor_by_code(params):
    k = params[0]
    g = params[1]
    n_windows = params[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g['flow_in_ratio1'] = g['flow_in'].rolling(window=n_windows).sum()/g['turnoverValue'].rolling(window=n_windows).sum()
    g['flow_in_ratio1'] = g['flow_in_ratio1'].shift(1)
    g['code'] = k
    return g

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime) -> pd.DataFrame:
    n_windows = 3
    trade_date_list = DateUtilities.makeSchedule(begin_date,end_date,'1b','china.sse')
    res = []
    
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        res = p.map(calc_med_factor_by_day, trade_date_list)
    res = pd.concat(res).reset_index()

    grouped = res.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        flow_in_ratio1_res = p.map(calc_factor_by_code, grouped_list)
    
    flow_in_ratio1_df = pd.concat(flow_in_ratio1_res).reset_index(drop=True)
    flow_in_ratio1_df = flow_in_ratio1_df.dropna()
    return flow_in_ratio1_df.loc[:, ['trade_date', 'code', 'flow_in_ratio1']]

if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)
    print(calc_factor(begin_date,end_date))