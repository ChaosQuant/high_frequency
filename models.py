# -*- coding: utf-8 -*-
from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String, Text, Boolean, text, JSON,TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class DailyHighFrequency(Base):
    __tablename__ = 'daily_high_frequency'
    trade_date = Column(DateTime, primary_key=True, nullable=False)
    code = Column(Integer, primary_key=True, nullable=False)
    flow_in_ratio1 = Column(Float(53))
    hf_volatility = Column(Float(53))
    improved_reversal = Column(Float(53))
    trend_strength = Column(Float(53))
    volume_price_corr = Column(Float(53))
    volume_ratio = Column(Float(53))

    
class Market5MinBar(Base):
    __tablename__ = 'market_bar_5mins'
    trade_date = Column(TIMESTAMP, primary_key=True, nullable=False)
    code = Column(Integer, primary_key=True, nullable=False)
    bar_time = Column(String)
    close_price = Column(Float(53))
    high_price = Column(Float(53))
    low_price = Column(Float(53))
    open_price = Column(Float(53))
    total_value = Column(Float(53))
    total_volume = Column(Float(53))
    vwap = Column(Float(53))
    twap = Column(Float(53))
    accumadjfactor = Column(Float(53))
    
class Market(Base):
    __tablename__ = 'market'
    __table_args__ = (
        Index('market_idx', 'trade_date', 'code', unique=True),
    )

    trade_date = Column(DateTime, primary_key=True, nullable=False)
    code = Column(String, primary_key=True, nullable=False)
    secShortName = Column(String(10))
    exchangeCD = Column(String(4))
    preClosePrice = Column(Float(53))
    actPreClosePrice = Column(Float(53))
    openPrice = Column(Float(53))
    highestPrice = Column(Float(53))
    lowestPrice = Column(Float(53))
    closePrice = Column(Float(53))
    turnoverVol = Column(BigInteger)
    turnoverValue = Column(Float(53))
    dealAmount = Column(BigInteger)
    turnoverRate = Column(Float(53))
    accumAdjFactor = Column(Float(53))
    negMarketValue = Column(Float(53))
    marketValue = Column(Float(53))
    chgPct = Column(Float(53))
    PE = Column(Float(53))
    PE1 = Column(Float(53))
    PB = Column(Float(53))
    isOpen = Column(Integer)
    vwap = Column(Float(53))