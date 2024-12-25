import datetime
import time

import sqlalchemy
import streamlit as st
import pandas as pd
import akshare as ak
from sqlalchemy.orm import sessionmaker
from utilities import Market, stock_markert, get_price

# 连接本地数据库
st.session_state['engine'] = sqlalchemy.create_engine('sqlite:///stockwatcher.db', echo=False)
Session = sessionmaker(bind=st.session_state['engine'])
st.session_state['session'] = Session()

st.set_page_config(page_title="更新行情数据")
# st.sidebar.success("可切换不同页面，进行不同分析")

st.header("欢迎使用股票监控器！", divider=True)

# 行情数据处理
st.markdown("### 行情数据准备")
# 查询最新行情日期
last_date = pd.to_datetime(
    st.session_state['session'].query(Market.tradeDate).order_by(Market.tradeDate.desc()).first()[0],
    format="%Y-%m-%d").date()
st.write(f"最新行情数据日期为：{last_date}")
# 获取交易日
trade = ak.tool_trade_date_hist_sina()['trade_date']
start_date = trade[trade[trade == last_date].index + 1].item()  # datetime.date
end_date = datetime.datetime.today().date()

if start_date > end_date:
    getTrade = st.button("更新行情数据", disabled=True, )
else:
    getTrade = st.button("更新行情数据")
# 执行数据同步
if getTrade:
    with st.spinner(f"等待爬虫抓取全A[{start_date} 至 {end_date}]数据，请勿关闭页面，约耗时1h..."):
        # marketdata, _ = update_marketdata_daily(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
        stock = ak.stock_zh_a_spot_em()[['代码', '名称']]
        stock['market'] = stock['代码'].apply(lambda x: stock_markert(x))
        stock = stock[stock['market'] != 'bj']
        my_bar = st.progress(0, text="行情数据爬取中")

        results = []
        error_p = []
        error = []
        for percent, i in enumerate(stock['代码'].to_list()):
            try:
                _b = get_price(i, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
                results.append(_b)
            except Exception as e:
                error.append(i)
                error_p.append({'code': i, 'start': start_date, 'end': end_date, 'error': e})
            _p = percent / len(stock['代码'].to_list())
            my_bar.progress(_p, text='行情数据爬取中')

        time.sleep(0.5)
        my_bar.empty()

        if len(error) > 0:
            time.sleep(10)
            for code in error:
                try:
                    _b = get_price(i, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
                    results.append(_b)
                except Exception as e:
                    st.write(code)
        marketdata = pd.concat(results)
        marketdata = marketdata.rename(columns={
            '日期': 'tradeDate', '股票代码': 'ticker', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volumn', '成交额': 'amount',
            '振幅': 'amplitude', '涨跌幅': 'perf_percent', '涨跌额': 'perf_amount',
            '换手率': 'turnover'
        })
        marketdata['createtime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        marketdata['updatetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        d_type = {
            'tradeDate': sqlalchemy.types.Text, 'ticker': sqlalchemy.types.Text, 'open': sqlalchemy.types.REAL,
            'close': sqlalchemy.types.REAL, 'high': sqlalchemy.types.REAL, 'low': sqlalchemy.types.REAL,
            'volumn': sqlalchemy.types.REAL, 'amount': sqlalchemy.types.REAL, 'amplitude': sqlalchemy.types.REAL,
            'perf_percent': sqlalchemy.types.REAL, 'perf_amount': sqlalchemy.types.REAL,
            'turnover': sqlalchemy.types.REAL,
            'createtime': sqlalchemy.types.Text, 'updatetime': sqlalchemy.types.Text
        }
        try:
            marketdata.to_sql('marketData_daily', st.session_state['engine'], if_exists='append', index=False,
                              dtype=d_type,
                              chunksize=100000)
            st.success(f"行情数据下载完成, 共写入{marketdata.shape[0]}行数据")
        except Exception as e:
            print(e)
st.divider()





