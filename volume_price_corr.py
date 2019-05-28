# coding=utf-8

import pdb
import datetime
import pandas as pd
import numpy as np
import sqlalchemy as sa
import multiprocessing
from dateutil.relativedelta import relativedelta
from sqlalchemy import select, and_, func
from PyFin.api import DateUtilities
from models import Market5MinBar
import config

def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    g = g.sort_values(by='bar_time', ascending=True)
    corr = np.corrcoef(g.close_price, g.total_volume)[0, 1]
    return {'corr':corr,'code':k[1],'trade_date':k[0]}

def calc_factor_by_mean(unit: list) -> pd.DataFrame:
    k = unit[0]
    g = unit[1]
    n_windows = unit[2]
    g = g.sort_values(by='trade_date', ascending=True)
    g[str(n_windows) + '_volume_price_corr'] = g['corr'].rolling(window=n_windows).mean()
    return g[['trade_date',str(n_windows) + '_volume_price_corr','code']].dropna()
    
def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime,
               **kwargs) -> pd.DataFrame:
    n_windows = kwargs['windows']
    table = Market5MinBar
    
    
    #计算交易日 涉及到均值计算，故要获取 begin_date 前n_windows数据
    interval_day = '-%d'%(n_windows) + 'd'
    temp_trade_date_list = DateUtilities.makeSchedule(begin_date-relativedelta(days=int(2 * n_windows)), 
                                                      begin_date,'1b','china.sse')
    temp_trade_date_list.sort(reverse=False)
    temp_trade_date_list = temp_trade_date_list[-n_windows:]
    trade_date_list = DateUtilities.makeSchedule(temp_trade_date_list[0], end_date,'1b','china.sse')
    
    
    
    
    conn = sa.create_engine(config.DX_DB)
    query = select([table.trade_date,table.code,table.bar_time,table.close_price,
                    table.total_volume]).where(and_(Market5MinBar.trade_date >= temp_trade_date_list[0],
                                                           Market5MinBar.trade_date <= end_date))
    
    df = pd.read_sql(query,conn)
    
    grouped_list = []
    grouped = df.groupby(['trade_date','code'])
    for k, g in grouped:
        grouped_list.append([k,g])
        
    ##多进程计算
    cpus = multiprocessing.cpu_count()
    corr_res = []
    with multiprocessing.Pool(processes=cpus) as p:
        corr_res = p.map(calc_factor_by_code, grouped_list)
    corr_res = pd.DataFrame(corr_res)
    
    
    
    #计算均值
    grouped = corr_res.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    corr_res = []
    with multiprocessing.Pool(processes=cpus) as p:
        corr_res = p.map(calc_factor_by_mean, grouped_list)
    corr_df = pd.concat(corr_res).reset_index(drop=True)
    return corr_df
    
   
if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)
    calc_factor(begin_date,end_date)
    