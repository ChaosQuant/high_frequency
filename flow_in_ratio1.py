# coding=utf-8

import pdb
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from PyFin.api import DateUtilities
from models import Market5MinBar, Market

def calc_med_factor_by_day(unit: list):
    k = unit[0]
    g = unit[1]
    g = g.sort_values(by='bar_time', ascending=True)
    close_diff = (g.close_price - g.close_price.shift(1)).fillna(0)
    close_diff_sign = close_diff.map(lambda x: np.sign(x))
    flow_in = (g.close_price*g.total_volume*close_diff_sign).sum()
    # flow_in = (g.total_value*close_diff).sum() # another method for inflow
    return {'flow_in':flow_in,'code':k[1],'trade_date':k[0]}

def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    n_windows = unit[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g[str(n_windows)+'_flow_in_ratio1'] = g['flow_in'].rolling(window=n_windows).sum()/g['turnoverValue'].rolling(window=n_windows).sum()
    g[str(n_windows)+'_flow_in_ratio1'] = g[str(n_windows)+'_flow_in_ratio1'].shift(1)
    return g.loc[:,['trade_date','code',str(n_windows)+'_flow_in_ratio1']].dropna()

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime) -> pd.DataFrame:
    # param for mean
    n_windows = 4
    conn = sa.create_engine('postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha')
    
    # bars for 5-mins
    table = Market5MinBar
    query = select([Market5MinBar.trade_date,Market5MinBar.code,
                    Market5MinBar.bar_time,Market5MinBar.close_price,
                    Market5MinBar.total_volume, Market5MinBar.total_value
                   ]).where(and_(Market5MinBar.trade_date >= begin_date,
                                                           Market5MinBar.trade_date <= end_date))
    res = pd.read_sql(query, conn)
    grouped_list = []
    grouped = res.groupby(['trade_date','code'])
    
    for k, g in grouped:
        grouped_list.append([k,g])
        
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        flow_in_res = p.map(calc_med_factor_by_day, grouped_list)
    flow_in_df= pd.DataFrame(flow_in_res)
    
    # daily value
    table2 = Market
    query2 = select([table2.trade_date,table2.code,table2.turnoverValue
                    ]).where(and_(Market.trade_date >= begin_date,
                                                           Market.trade_date <= end_date))
    res2 = pd.read_sql(query2,conn)
    flow_in_df = flow_in_df.merge(res2, how='inner', on=['trade_date','code'])
    
    # calculate factors
    grouped = flow_in_df.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    with multiprocessing.Pool(processes=cpus) as p:
        flow_in_ratio1_res = p.map(calc_factor_by_code, grouped_list)
    
    flow_in_ratio1_df = pd.concat(flow_in_ratio1_res).reset_index(drop=True)    
    return flow_in_ratio1_df

if __name__ == '__main__':
    begin_date = datetime.datetime(2019,1,1)
    end_date = datetime.datetime(2019,2,10)
    print(calc_factor(begin_date, end_date))