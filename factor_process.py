# -*- coding: utf-8 -*-

import pdb
import datetime
import sqlalchemy as sa
import pandas as pd
import uqer
from uqer import DataAPI
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, and_, or_, MetaData, delete,update
import config
from models import DailyHighFrequency
from flow_in_ratio1 import calc_factor as flow_in_ratio1_calc_factor
from hf_volatility import calc_factor as hf_volatility_calc_factor
from volume_price_corr import calc_factor as volume_price_corr_calc_factor
from volume_ratio import calc_factor as volume_ratio_calc_factor
from improved_reversal import calc_factor as improved_reversal_calc_factor
from trend_strength import calc_factor as trend_strength_calc_factor

class FactorProess(object):
    
    def __init__(self):
        #目标数据库
        self._destination = sa.create_engine(config.DK_DB)
        self._destsession = sessionmaker( bind=self._destination, autocommit=False, autoflush=True)
    
    def update_destdb(self, table_name, sets):
        sets = sets.where(pd.notnull(sets), None)
        sql_pe = 'INSERT INTO {0} SET'.format(table_name)
        updates = ",".join( "{0} = :{0}".format(x) for x in list(sets) )
        sql_pe = sql_pe + '\n' + updates
        sql_pe = sql_pe + '\n' +  'ON DUPLICATE KEY UPDATE'
        sql_pe = sql_pe + '\n' + updates
        session = self._destsession()
        print('update_destdb')
        for index, row in sets.iterrows():
            dict_input = dict( row )
            dict_input['trade_date'] = dict_input['trade_date'].to_pydatetime()
            session.execute(sql_pe, dict_input)
        session.commit()
        session.close()
    
    
    def update_stock(self):
        pdb.set_trace()
        stock_df = DataAPI.EquGet(secID=u"",ticker=u"",equTypeCD=u"A",listStatusCD=u"",field=u"secID,ticker",
                                  pandas="1")
        stock_df = stock_df[:-1]
        stock_df = stock_df.rename(columns={'ticker':'code'})
        stock_df['code'] = stock_df['code'].apply(lambda x : int(x))
        stock_df.to_csv('stock_info.csv',encoding='UTF-8')
        
    def load_stock(self):
        pdb.set_trace()
        df = pd.read_csv('stock_info.csv', index_col=0)
        return df
    
    def on_work(self):
        #获取股票信息
        stock_df = self.load_stock()
        begin_date = datetime.datetime(2018, 1, 1)
        end_date = datetime.datetime(2018, 1, 10)
        
        flow_in_ratio1 = flow_in_ratio1_calc_factor(begin_date, end_date, 
                   windows=3).merge(stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        self.update_destdb('daily_high_frequency', flow_in_ratio1)
        
        trend_strength = trend_strength_calc_factor(begin_date, end_date, 
                               windows=4).merge(stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        self.update_destdb('daily_high_frequency', trend_strength)
        
        '''
        improved_reversal = improved_reversal_calc_factor(begin_date, end_date, 
                               windows=4).merge(stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        self.update_destdb('daily_high_frequency', improved_reversal)
        
        
        hf_volatility = hf_volatility_calc_factor(begin_date, end_date, 
                   windows=3).merge(stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        self.update_destdb('daily_high_frequency', hf_volatility)
        
        volume_price_corr = volume_price_corr_calc_factor(begin_date, end_date,
                   windows=3).merge(stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        self.update_destdb('daily_high_frequency', volume_price_corr)
       
        volume_ratio = volume_ratio_calc_factor(begin_date, end_date).merge(
            stock_df, on=['code']).drop(['code'],axis=1).rename(columns={'secID':'code'})
        grouped = volume_ratio.groupby(['bar_time'])
        for k, g in grouped:
            new_columns = str(k).replace(':','') + '_volume_ratio'
            g = g.rename(columns={'ratio': new_columns})[['trade_date','code',new_columns]]
            self.update_destdb('daily_high_frequency', g)
        '''
        print('----->')
        
        
        

if __name__ == '__main__':
    client = uqer.Client(token=config.UQUER_TOKEN)
    factor_proess = FactorProess()
    print(factor_proess.load_stock())
