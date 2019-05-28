# coding=utf-8
import pdb
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from sqlalchemy.pool import NullPool
from PyFin.api import DateUtilities
import config
from models import Market5MinBar,Market

def calc_factor_by_day(trade_date: datetime.datetime) -> pd.DataFrame:
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB, poolclass=NullPool)
    #获取分钟K
    query = select([Market5MinBar.trade_date,Market5MinBar.code,Market5MinBar.bar_time,Market5MinBar.close_price,
                    Market5MinBar.total_volume]).where(and_(Market5MinBar.trade_date==trade_date))
    min5_df = pd.read_sql(query,conn)
    
    
    # 获取日K得到成交量
    query = select([Market.trade_date,Market.code,Market.closePrice,
                    Market.turnoverVol]).where(and_(Market.trade_date==trade_date))
    daily_df = pd.read_sql(query,conn)
    daily_df = daily_df.set_index('code')
    
    grouped = min5_df.groupby('code')

    min30_list = []
    for k, g in grouped:
        g = g.sort_values(by='bar_time', ascending=True)[1:].reset_index(drop=True)
        group_num = int(len(g)/6)
        daily_volume = daily_df.loc[k].turnoverVol
        for i in range(0,group_num):
            unit = g.loc[i*6:i*6+5]
            total_volume = unit.total_volume.sum()
            last_dict = unit.to_dict(orient='records')[-1]# Series赋值效率低 转为dict进行赋值提高效率
            last_dict['total_volume'] = total_volume
            last_dict['ratio_volume'] =  0 if daily_volume==0 else float(total_volume)/float(daily_volume)
            min30_list.append(last_dict)
    min30_bar = pd.DataFrame(min30_list)
    return min30_bar


def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    g['ratio'] = g['ratio_volume'].rolling(window=2).mean()
    g['code'] = k[1]
    g['bar_time'] = k[0]
    return g[['bar_time','trade_date','code','ratio']].dropna()
    
def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime,
                **kwargs) -> pd.DataFrame:
    trade_date_list = DateUtilities.makeSchedule(begin_date,end_date,'1b','china.sse')
    res = []
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        res = p.map(calc_factor_by_day, trade_date_list)
    res = pd.concat(res).reset_index(drop=True)
    grouped = res.groupby(['bar_time','code'])
    ratio_res = []
    grouped_list = []
    
    ##分组计算-> 1. 多进程  2.自定义函数 apply  3.自定义函数 agg  
    ## 性能统计测算
    for k, g in grouped:
        #放入多进程中
        grouped_list.append([k,g])
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        ratio_res = p.map(calc_factor_by_code, grouped_list)
    res = pd.concat(ratio_res).reset_index(drop=True)
    return res

if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 4)
    end_date = datetime.datetime(2019, 1, 8)
    print(calc_factor(begin_date,end_date))