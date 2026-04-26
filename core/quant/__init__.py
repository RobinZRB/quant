import baostock as bs
import requests
import pandas as pd
import json


def get_us_stock_combined(symbol, start_date, end_date):
    """
    组合多种数据源获取美股数据
    """
    # 方法1: 尝试从公开API获取
    try:
        # 使用Alpha Vantage的替代接口（通过国内镜像）
        url = f"https://www.alphavantage.co/query"
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': 'demo',  # 使用demo key或注册获取
            'outputsize': 'full'
        }

        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if 'Time Series (Daily)' in data:
            time_series = data['Time Series (Daily)']
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df = df.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })

            # 转换数据类型
            for col in df.columns:
                df[col] = pd.to_numeric(df[col])

            df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            # 筛选日期范围
            mask = (df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))
            df = df[mask]

            return df

    except Exception as e:
        print(f"API获取失败: {str(e)}")

    return pd.DataFrame()