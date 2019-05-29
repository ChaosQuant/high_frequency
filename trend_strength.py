# coding=utf-8

import pdb
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from dateutil.relativedelta import relativedelta
from PyFin.api import DateUtilities
from models import Market5MinBar,Market

def calc_factor_by_day(unit: list):
    k = unit[0]
    g = unit[1]
    g = g.sort_values(by='bar_time', ascending=True)
    trend_strength = (g.close_price.iloc[-1]-g.close_price.iloc[0])/g.close_price.diff().fillna(0).map(np.abs).sum()
    return {'trend_strength':trend_strength, 
           'code':k[1],'trade_date':k[0]}

def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    n_windows = unit[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g[str(n_windows)+'_trend_strength'] = g['trend_strength'].rolling(window=n_windows).mean().shift(1)
    return g.loc[:,['trade_date','code',str(n_windows)+'_trend_strength']].dropna()

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime,
               **kwargs) -> pd.DataFrame:
    # param for mean
    n_windows = kwargs['windows']
    table = Market5MinBar
    conn = sa.create_engine('postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha')
    
#     pdb.set_trace()    
    # n_windows more days needed to prevent NaNs during rolling calcaulation
    temp_trade_date_list = DateUtilities.makeSchedule(begin_date-relativedelta(days=int(2 * n_windows)), 
                                                      begin_date,'1b','china.sse')
    temp_trade_date_list.sort(reverse=False)
    temp_trade_date_list = temp_trade_date_list[-n_windows:]
    # trade_date_list = DateUtilities.makeSchedule(temp_trade_date_list[0],end_date,'1b','china.sse')
    
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
        trend_strength_res = p.map(calc_factor_by_day, grouped_list)
    trend_strength_df= pd.DataFrame(trend_strength_res)
    
    # calculate rolling mean by date     
    grouped = trend_strength_df.groupby('code')
    grouped_list = []
    for k,g in grouped:
        grouped_list.append([k,g,n_windows])
    with multiprocessing.Pool(processes=cpus) as p:
        trend_strength_res = p.map(calc_factor_by_code, grouped_list)
    trend_strength_df = pd.concat(trend_strength_res).reset_index(drop=True)
    return trend_strength_df

if __name__ == '__main__':
    begin_date = datetime.datetime(2019,1,1)
    end_date = datetime.datetime(2019,1,10)
    print(calc_factor(begin_date, end_date, windows=4))