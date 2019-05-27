# coding=utf-8

import pdb
import datetime
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import select, and_, func
from PyFin.api import DateUtilities
from models import Market5MinBar

def calc_factor_by_day(trade_date: datetime.datetime) -> pd.DataFrame:
    table = Market5MinBar
    conn = sa.create_engine(config.DX_DB)
    query = select([table.trade_date,table.code,table.bar_time,table.close_price,
                    table.total_volume]).where(and_(table.trade_date==datetime.datetime(2019, 1, 10)))
    
    df = pd.read_sql(query,conn)
    grouped = df.groupby('code')
    corr_df = pd.DataFrame(columns=['corr'])
    for k, g in grouped:
        g = g.sort_values(by='bar_time', ascending=True)
        corr = np.corrcoef(g.close_price, g.total_volume)[0, 1]
        corr_df.loc[k, :] = corr
    corr_df = corr_df.fillna(0)
    corr_df.index.name = 'code'
    corr_df['trade_date'] = trade_date
    return corr_df

def calc_factor(begin_date: datetime.datetime,
               end_date: datetime.datetime) -> pd.DataFrame:
    n_windows = 4
    trade_date_list = DateUtilities.makeSchedule(begin_date,end_date,'1b','china.sse')
    res = []
    for trade_date in trade_date_list:
        corr_df = calc_factor_by_day(trade_date)
        res.append(corr_df)
    res = pd.concat(res).reset_index()
    #计算均值
    grouped = res.groupby('code')
    corr_res = []
    for k, g in grouped:
        g = g.sort_values(by='trade_date', ascending=True)
        g[str(n_windows) + '_corr'] = g['corr'].rolling(window=n_windows).mean()
        g['code'] = k
        corr_res.append(g.dropna())
    pdb.set_trace()
    corr_df = pd.concat(corr_res).reset_index()
    print('----')
    
   
if __name__ == '__main__':
    begin_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 1, 10)
    calc_factor(begin_date,end_date)
    