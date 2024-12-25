import akshare as ak
import pandas as pd
import time
import threading
import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, REAL, Text
from retrying import retry


def stock_markert(code):
    """
    根据股票代码判断市场类型
    :param code:
    :return:
    """
    if code[0] == '3' or code[0] == '0':
        return 'sz'
    elif code[0] == '6':
        return 'sh'
    else:
        return 'bj'


def update_marketdata_daily(start_date: str, end_date: str):
    stock = ak.stock_zh_a_spot_em()[['代码', '名称']]
    stock['market'] = stock['代码'].apply(lambda x: stock_markert(x))
    stock = stock[stock['market'] != 'bj']

    results = []
    errorlist = []
    errorlist2 = []

    def _get_data(codelist, errorlist: list):
        for i in stock['代码'].to_list():
            try:
                _b = ak.stock_zh_a_hist(symbol=i, period='daily', start_date=start_date,
                                        end_date=end_date,
                                        adjust='hfq', timeout=30)
            except Exception as e:
                print(e)
                errorlist.append(i)
                continue
            if _b.empty:
                continue
            else:
                results.append(_b)

    _get_data(stock['代码'].to_list(), errorlist)

    if len(errorlist) > 0:
        time.sleep(10)
        _get_data(errorlist, errorlist2)

    a = pd.concat(results)
    a.reset_index(inplace=True, drop=True)
    a = a.rename(columns={
        '日期': 'tradeDate', '股票代码': 'ticker', '开盘': 'open', '收盘': 'close',
        '最高': 'high', '最低': 'low', '成交量': 'volumn', '成交额': 'amount',
        '振幅': 'amplitude', '涨跌幅': 'perf_percent', '涨跌额': 'perf_amount',
        '换手率': 'turnover'
    })
    a['createtime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    a['updatetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return a, errorlist2


@retry(stop_max_attempt_number=3, wait_fixed=3000)
def get_price(code, start_date: str, end_date: str):
    _b = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date,
                            end_date=end_date, adjust='hfq', timeout=30)
    return _b


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.result = None
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


"""
    创建ORM映射对象
"""
Base = declarative_base()


class Market(Base):
    __tablename__ = 'marketData_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tradeDate = Column(Text)
    ticker = Column(Text)
    open = Column(REAL)
    close = Column(REAL)
    high = Column(REAL)
    low = Column(REAL)
    amount = Column(REAL)
    amplitude = Column(REAL)
    perf_percent = Column(REAL)
    perf_amount = Column(REAL)
    createtime = Column(Text)
    updatetime = Column(Text)
    volumn = Column(REAL)
    turnover = Column(REAL)

    def __repr__(self):
        return "<Market(name=marketData_daily, comment=记录全A日行情数据)>"


class GoldenVersion(Base):
    __tablename__ = 'golden_version'
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Text)
    main_bottom = Column(REAL)
    main_upper = Column(REAL)
    star_bottom = Column(REAL)
    star_upper = Column(REAL)
    amplitude = Column(REAL)
    v = Column(REAL)
    box_bottom = Column(REAL)
    box_upper = Column(REAL)
    start_window = Column(REAL)
    end_window = Column(REAL)
    tao = Column(REAL)
    createtime = Column(Text)
    updatetime = Column(Text)

    def __repr__(self):
        return "<Market(name=golden_version, comment=记录黄金台参数版本)>"


class GoldenResults(Base):
    __tablename__ = 'golden_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(Text)
    signal = Column(Text)
    version = Column(Text)
    createtime = Column(Text)
    updatetime = Column(Text)

    def __repr__(self):
        return "<Market(name=golden_results, comment=记录黄金台识别结果)>"
