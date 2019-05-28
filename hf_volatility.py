# coding=utf-8

import pdb
import config
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from PyFin.api import DateUtilities
from models import Market5MinBar,Market


def calc_factor_by_code(unit: list):
    k = unit[0]
    g = unit[1]
    g['prev_close'] = g['close_price'].shift(1)
    g = g.dropna()
    g['v'] = np.log(g['close_price']/g['prev_close'])
    down_g = g[g['v']<0]
    v = np.power(g['v'],2).sum()
    down_volatility = 0 if v == 0 else np.power(down_g['v'],2).sum() / v
    return {'down_volatility':down_volatility,
           'code':k[1],'trade_date':k[0]}

def calc_factor_mean(unit: list):
    k = unit[0]
    g = unit[1]
    n_windows = unit[2]
    g[str(n_windows) + '_down_volatility'] = g['down_volatility'].fillna(0).rolling(window=n_windows).mean()
    return g[[str(n_windows) + '_down_volatility','code','trade_date']].dropna()
    
def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime,
               **kwargs) -> pd.DataFrame:
    n_windows = kwargs['windows']
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB)
    #获取分钟K
    query = select([Market5MinBar.trade_date,Market5MinBar.code,Market5MinBar.bar_time,Market5MinBar.close_price,
                    Market5MinBar.total_volume]).where(and_(Market5MinBar.trade_date >= begin_date,
                                                           Market5MinBar.trade_date <= end_date))
    res = pd.read_sql(query,conn)
    grouped_list = []
    grouped = res.groupby(['trade_date','code'])
    for k, g in grouped:
        grouped_list.append([k,g])
    cpus = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=cpus) as p:
        volatility_res = p.map(calc_factor_by_code, grouped_list)
    volatility_res = pd.DataFrame(volatility_res)
    #计算均值
    grouped = volatility_res.groupby('code')
    grouped_list = []
    for k, g in grouped:
        grouped_list.append([k,g,n_windows])
    with multiprocessing.Pool(processes=cpus) as p:
        res = p.map(calc_factor_mean, grouped_list)
    res = pd.concat(res).reset_index(drop=True)
    return res
    
    
if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 4)
    end_date = datetime.datetime(2019, 1, 8)
    print(calc_factor(begin_date,end_date))