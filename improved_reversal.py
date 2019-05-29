# coding=utf-8

import pdb
import config
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from dateutil.relativedelta import relativedelta
from PyFin.api import DateUtilities
from models import Market5MinBar, Market

def calc_factor_by_day(unit: list):
    k = unit[0]
    g = unit[1]
    g = g.sort_values(by='bar_time', ascending=True)
    improved_reversal = (g.close_price.iloc[-1]-g.close_price.iloc[6])/g.close_price.iloc[6]
    return {'improved_reversal':improved_reversal,
            'code':k[1],'trade_date':k[0]}

def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    n_windows = unit[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g[str(n_windows)+'_improved_reversal'] = g['improved_reversal'].rolling(window=n_windows).mean().shift(1)
    return g.loc[:,['trade_date','code',str(n_windows)+'_improved_reversal']].dropna()

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime,
               **kwargs) -> pd.DataFrame:
    # param for mean
    n_windows = kwargs['windows']
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB)

    #     pdb.set_trace()    
    # n_windows more days needed to prevent NaNs during rolling calcaulation
    temp_trade_date_list = DateUtilities.makeSchedule(begin_date-relativedelta(days=int(2 * n_windows)), 
                                                      begin_date,'1b','china.sse')
    temp_trade_date_list.sort(reverse=False)
    temp_trade_date_list = temp_trade_date_list[-n_windows:]
    
    # bars for 5-mins
    query = select([Market5MinBar.trade_date,Market5MinBar.code,Market5MinBar.bar_time,Market5MinBar.close_price,
                    ]).where(and_(Market5MinBar.trade_date >= temp_trade_date_list[0],
                                                           Market5MinBar.trade_date <= end_date))
    res = pd.read_sql(query, conn)
    grouped_list = []
    grouped = res.groupby(['trade_date','code'])

    for k, g in grouped:
        grouped_list.append([k,g])
    
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        improved_reversal_res = p.map(calc_factor_by_day, grouped_list)
    improved_reversal_df= pd.DataFrame(improved_reversal_res)
    
    # calculate rolling mean by date
    grouped = improved_reversal_df.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    with multiprocessing.Pool(processes=cpus) as p:
        improved_reversal_res = p.map(calc_factor_by_code, grouped_list)
    improved_reversal_df = pd.concat(improved_reversal_res).reset_index(drop=True)
    return improved_reversal_df

if __name__ == '__main__':
    begin_date = datetime.datetime(2019,1,1)
    end_date = datetime.datetime(2019,1,10)
    print(calc_factor(begin_date, end_date, windows=4))