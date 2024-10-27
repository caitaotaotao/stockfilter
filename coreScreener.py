"""
    * @Author: caitao
    * @Date: 2024-10-26
    * @Project: 股票筛选器

"""
import datetime
import math

import akshare as ak
from utilities import MyThread
from tqdm import tqdm


def get_share():
    """
    获取当前沪深京股票列表
    :return: pd.DataFrame(column=['代码', '名称'])
    """
    a = ak.stock_zh_a_spot_em()
    return a[['代码', '名称']]


def get_tradingPeriod(date1, n):
    """
    通过输入日期，返回给定长度交易时间段
    :param n: int
    :param date1: datetime.date
    :return: start_date, end_date
    """
    tradings = ak.tool_trade_date_hist_sina()
    if date1 in tradings:
        end_date = date1

    else:
        tradings = tradings[tradings['trade_date'] < date1]
        tradings['delta'] = tradings['trade_date'] - date1
        end_date = tradings.iloc[
            tradings[tradings['delta'] == tradings['delta'].max()].index[0], 0
        ]

    start_date = tradings.iloc[
        tradings[tradings['trade_date'] == end_date].index[0] - n + 1,
        0
    ]
    return start_date, end_date


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


class goldenPlatform:
    def __init__(self, stockpool, amplitude=20, boxrange=0.1):
        self.amplitude = amplitude
        self.boxrange = boxrange
        self.stockpool = stockpool

        today = datetime.date.today()
        start_date, end_date = get_tradingPeriod(today, 10)
        self.start_date = datetime.datetime.strftime(start_date, '%Y%m%d')
        self.end_date = datetime.datetime.strftime(end_date, '%Y%m%d')
        return

    def _kCharacterization(self, stockprice):
        """
        刻画个股黄金台样式，第4-10根K柱
            振幅：默认振幅<=16%
            箱型结构：4-10的开盘价和收盘价，在第一个柱的[开盘价，收盘价*1.05]
        :param stockprice:
        :return:
        """

        amplitude_list = stockprice.iloc[3:, 8].to_list()
        if all(_ <= self.amplitude for _ in amplitude_list):
            # 确定箱体上下界
            upper = stockprice.iloc[0, 3] * (1 + self.boxrange)
            lower = stockprice.iloc[0, 2] * (1 - self.boxrange)
            # 4-10 K线柱的开盘价、收盘价合集
            a1 = stockprice.iloc[3:, 2].to_list()
            a2 = stockprice.iloc[3:, 3].to_list()
            a = a1 + a2
            if all(lower <= _ <= upper for _ in a):
                return True
            else:
                return False
        else:
            return False

    def _stockFilter(self, code, increaseType):
        """
        每一只股票进行过滤
        :return:
        """
        try:
            stockprice = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=self.start_date,
                                            end_date=self.end_date,
                                            adjust='hfq', timeout=30)  # 后复权、超时设置为30s
        except Exception as e:
            print(e)
            return False

        if stockprice.empty:
            return False
        elif stockprice.shape[0] < 10:
            return False

        # 黄金台主体逻辑
        # 第一个交易日为涨停
        stockprice.sort_values(by='日期', ascending=True, inplace=True)  # 设置按日期升序排列
        if increaseType == 'radical':
            if stockprice.iloc[0, 9] > 19.99 and self._kCharacterization(stockprice):
                return True
            else:
                return False
        else:
            if stockprice.iloc[0, 9] > 9.99 and self._kCharacterization(stockprice):
                return True
            else:
                return False

    def golden_recon(self):
        """
        黄金台模式识别1：选择具有10个交易日，且第一个交易日为涨停
        :return:
        """
        result = []
        for i in tqdm(range(self.stockpool.shape[0])):
            _temp = self.stockpool.iloc[i, :]['qutoa']
            _code = self.stockpool.iloc[i, 0]
            if self._stockFilter(code=_code, increaseType=_temp):
                result.append(_code)
            else:
                continue
        return result


def multi_goldenRegcon(threds=20):
    """
        多线程模式
        :param threds: 线程数量
        :return:
        """
    results = []
    stock = ak.stock_zh_a_spot_em()[['代码', '名称']]
    # 过滤北交所
    stock['market'] = stock['代码'].apply(lambda x: stock_markert(x))
    stock = stock[stock['market'] != 'bj']
    # 设置涨跌幅标志
    stock['qutoa'] = stock['代码'].apply(lambda x: stockIncrease_type(x))

    f = locals()
    for i in range(10):
        delta = math.ceil(stock.shape[0] / threds)
        stockpool = stock.iloc[i * delta: (i + 1) * delta, :]
        f['gold_' + str(i)] = goldenPlatform(stockpool=stockpool)
        f['n_' + str(i)] = MyThread(f['gold_' + str(i)].golden_recon)

    for i in range(10):
        f['n_' + str(i)].start()

    for i in range(10):
        f['n_' + str(i)].join()

    # 获取结果
    for i in range(10):
        _result = f['n_' + str(i)].get_result()
        results.extend(_result)
    return results


def only_goldenRegcon():
    stock = ak.stock_zh_a_spot_em()[['代码', '名称']]
    # 过滤北交所
    stock['market'] = stock['代码'].apply(lambda x: stock_markert(x))
    stock = stock[stock['market'] != 'bj']
    # 设置涨跌幅标志
    stock['qutoa'] = stock['代码'].apply(lambda x: stockIncrease_type(x))

    test = goldenPlatform(stock)
    return test.golden_recon()


if __name__ == '__main__':
    result = multi_goldenRegcon()
    print(result)
