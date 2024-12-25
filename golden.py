"""
    @author: caitao
    @Date: 2024-11-30
    @feature:
        * 通过本地数据库取数，进行黄金台识别
    @update:
        * 修改匹配方法，先过滤涨停日期行索引，再判断后续规则
"""
import datetime

import pandas as pd
import math
import sqlalchemy
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from utilities import Market, MyThread
from tqdm import tqdm

num_threads = 20

engine = sqlalchemy.create_engine('sqlite:///stockwatcher.db', echo=False, max_overflow=20, pool_size=20)
Session = sessionmaker(bind=engine)
session = Session()


def stockIncrease_type(code):
    """
    根据股票代码判断涨跌停办要求
    :param code:
    :return:
    """
    if code[0] == '3' or code[0:3] == '688':
        return 'radical'
    else:
        return 'normal'


def get_stocks():
    """
    return: list of tuple('ticker'，)
    """
    return pd.read_sql(session.query(Market.ticker.distinct()).statement, session.bind)


def golden_filter(price, start_window, amplitude=15.00, boxrange_bottom=-0.01, boxrange_upper=0.8, v=0.6):
    if price.shape[0] < start_window:
        return False
    # 振幅合集
    amplitude_list = price.iloc[1:, 7].to_list()
    price['vollage'] = abs(price['open'] - price['close'])
    if all(_ <= amplitude for _ in amplitude_list):
        # 确定箱体上下界
        _close1 = price.iloc[0, 3]
        _open1 = price.iloc[0, 2]
        _v1 = price.iloc[0, -1]
        # 2-窗口末尾 K线柱的开盘价、收盘价合集
        a = price.iloc[1:, 2].to_list() + price.iloc[1:, 3].to_list()
        # 箱体上下限
        bottom = _v1 * boxrange_bottom + _open1
        upper = _v1 * boxrange_upper + _close1
        # 柱体大小限制
        vollage = _v1 * v
        # 2-10K线柱体大小
        b = price.iloc[1:, -1].to_list()
        if (max(a) <= upper) and (min(a) >= bottom) and (max(b) <= vollage) and (_close1 > _open1):
            return True
        else:
            return False
    else:
        return False


def trigger_v2(code, start_date, end_date, main_bottom=9.97, main_upper=11.00, star_bottom=19.97,
               star_upper=21.00,
               amplitude=15.00, boxrange_bottom=-0.01, boxrange_upper=0.8, v=0.6,
               tao=0.5, start_window=5, end_window=11):

    start_date = pd.to_datetime(start_date).date().strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')

    # 获取历史数据
    _stockprice = pd.read_sql(
        session.query(Market).filter(
            and_(Market.ticker == code, start_date <= Market.tradeDate, Market.tradeDate <= end_date)
        ).order_by(Market.tradeDate).statement,
        session.bind
    )

    _stockprice.drop(columns=['id', 'createtime', 'updatetime'], inplace=True)
    _stockprice.reset_index(drop=True, inplace=True)
    _stockprice['tradeDate'] = pd.to_datetime(_stockprice['tradeDate'])
    _stockprice['tradeDate'] = _stockprice['tradeDate'].dt.date

    # 过滤涨停
    _temp = stockIncrease_type(code)

    def _core(_stockprice, _temp):
        _results = []
        if _temp == 'normal':
            _raw = _stockprice[
                (_stockprice['perf_percent'] > main_bottom) & (_stockprice['perf_percent'] < main_upper)].index
        else:
            _raw = _stockprice[
                (_stockprice['perf_percent'] > star_bottom) & (_stockprice['perf_percent'] < star_upper)].index
        if _raw.empty:
            return _results
        else:
            for i in _raw:
                # 定义滑动窗口
                for w in range(int(start_window), int(end_window), 1):
                    _e = i + w
                    _price = _stockprice.iloc[i:_e, :].copy()

                    if len(_price) < start_window:
                        continue
                    else:
                        # 黄金台2-10柱体逻辑判断，固定窗口
                        _x = golden_filter(_price, start_window, amplitude, boxrange_bottom, boxrange_upper, v)
                        # 是否收敛判断
                        _upper = _price['high'].to_list()[1:]
                        _low = _price.iloc[1:, 5].to_list()
                        _distance = [a - b for a, b in zip(_upper, _low)]
                        if all(item == 0 for item in _distance):
                            _c = True
                        elif np.mean(_distance) < tao:
                            _c = True
                        else:
                            _c = False
                        # 判断条件：黄金台或者极致收敛任意触发
                        if _x or _c:
                            _results.append({'ticker': code, 'signal': _price.iloc[-1, 0]})
                            # print(f"---黄金台信号---{code}, 信号日：{_price.iloc[_e, 0]}")
                            continue
                        else:
                            continue
            return _results

    _result = _core(_stockprice, _temp)
    return _result


def goldenFilter_pool(codelist, start_date, end_date,
                      main_bottom=9.97, main_upper=11.00, star_bottom=19.97, star_upper=21.00,
                      amplitude=15.00, boxrange_bottom=-0.01, boxrange_upper=0.8, v=0.6,
                      tao=0.5, start_window=5, end_window=11):
    results = []
    for i in codelist:
        # try:
        #     _a = trigger_v2(i, start_date, end_date, main_bottom, main_upper, star_bottom, star_upper,
        #                     amplitude, boxrange_bottom, boxrange_upper, v,
        #                     tao, start_window, end_window)
        #     if len(_a) > 0:
        #         results.extend(_a)
        #     else:
        #         continue
        # except Exception as e:
        #     print(f"{i}, 错误码{e}")
        #     continue
        _a = trigger_v2(i, start_date, end_date, main_bottom, main_upper, star_bottom, star_upper,
                        amplitude, boxrange_bottom, boxrange_upper, v,
                        tao, start_window, end_window)
        if len(_a) > 0:
            results.extend(_a)
        else:
            continue
    return results


def goldenFilterThreads(start_date: datetime.date, end_date: datetime.date,
                        main_bottom=9.97, main_upper=11.00, star_bottom=19.97, star_upper=21.00,
                        amplitude=15.00, boxrange_bottom=-0.01, boxrange_upper=0.8, v=0.6,
                        tao=0.5, start_window=5, end_window=11):
    stock = get_stocks()
    results = []
    f = locals()
    for thread in range(num_threads):
        delta = math.ceil(stock.shape[0] / num_threads)
        _stocklist = stock['ticker'].to_list()[thread * delta: (thread + 1) * delta]
        f['n_' + str(thread)] = MyThread(goldenFilter_pool, args=(_stocklist, start_date, end_date,
                                                                  main_bottom, main_upper, star_bottom, star_upper,
                                                                  amplitude, boxrange_bottom, boxrange_upper, v,
                                                                  tao, start_window, end_window
                                                                  ))

    for t in range(num_threads):
        f['n_' + str(t)].start()

    for t in range(num_threads):
        f['n_' + str(t)].join()

    # 获取结果
    for t in range(num_threads):
        _result = f['n_' + str(t)].get_result()
        results.extend(_result)

    return results


def amplitude_convergence(code, start_date: str, end_date: str, increase_range=9.9, min_window=6, max_window=10,
                          tao=0.5):
    """
        识别窗口内股票是否出现极致压缩收敛的情况
        从阈值涨跌幅开始识别: 最新窗口尺寸计算累计收敛情况
        收敛识别算法：
            ** 上下振幅间距离 -> 移动平均 -> 标准差 -> 与阈值比较
            ** 上振幅：(当日最高-前日收盘价)/前日收盘价 - 1
            ** 下振幅：(当日最低 - 前日收盘价)/前日收盘价 - 1
        :param code:
        :param start_date: "YYYY-MM-DD"
        :param end_date: "YYYY-MM-DD"
        :param increase_range: 涨幅参数识别
        :param min_window: 窗口参数下限
        :param max_window: 窗口参数上限
        :param tao: 收敛阈值参数
    """
    _stockprice = pd.read_sql(
        session.query(Market).filter(
            and_(Market.ticker == code, start_date <= Market.tradeDate, Market.tradeDate <= end_date)
        ).order_by(Market.tradeDate).statement,
        session.bind
    )
    _stockprice.drop(columns=['id', 'createtime', 'updatetime'], inplace=True)

    # 过滤满足条件的出始索引
    _raw = _stockprice[_stockprice['perf_percent'] > increase_range].index
    if _raw.empty:
        pass
        return {}
    else:
        signals = []
        for s in _raw:
            results = {}
            _sdate = _stockprice.iloc[s, 0]
            for e in range(min_window, max_window, 1):
                _price = _stockprice.iloc[s:s + e, :].copy()  # 截取收敛判断
                _price.reset_index(drop=True, inplace=True)
                if _price.shape[0] < min_window - 1:
                    continue
                else:
                    _upper = _price['high'].to_list()[1:]
                    _low = _price.iloc[1:, 5].to_list()
                    _distance = [a - b for a, b in zip(_upper, _low)]
                    if np.mean(_distance) < tao:
                        _signal = _price.iloc[-1, 0]
                        results.update({_signal: np.mean(_distance)})
                        break
                    else:
                        continue
            if len(results) > 0:
                signals.append({_sdate: results})

        if len(signals) > 0:
            return {code: signals}
        else:
            return {}


if __name__ == '__main__':
    a = trigger_v2('001239', datetime.date(2024, 10, 8), datetime.date(2024, 12, 23))
    for i in a:
        print(i)
    # x = amplitude_convergence("600604", start_date="2019-01-01", end_date="2024-12-06", increase_range=9.9)
    # print(x)
    # s = pd.to_datetime("20190101").date()
    # e = pd.to_datetime("20241213").date()
    # x = goldenFilterThreads(s, e)
    # print(x)
