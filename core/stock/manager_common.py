import pandas as pd
from pandas import DataFrame

def standardize_stock_data(df: DataFrame, stock_code: str, stock_name: str, market) -> DataFrame:
    """
    标准化股票数据为统一的英文表头格式
    """
    # 添加股票基本信息
    df['stock_code'] = stock_code
    df['stock_name'] = stock_name
    df['market'] = market

    # 定义中文到英文的列名映射
    cn_to_en_map = {
        '日期': 'date',
        '交易日期': 'date',
        '开盘': 'open',
        '开盘价': 'open',
        '收盘': 'close',
        '收盘价': 'close',
        '最高': 'high',
        '最高价': 'high',
        '最低': 'low',
        '最低价': 'low',
        '成交量': 'volume',
        '成交量(股)': 'volume',
        '成交额': 'amount',
        '成交额(元)': 'amount',
        '成交额(港元)': 'amount',
        '振幅': 'amplitude',
        '振幅(%)': 'amplitude',
        '涨跌幅': 'change_pct',
        '涨跌幅(%)': 'change_pct',
        '涨跌额': 'change',
        '换手率': 'turnover_rate',
        '换手率(%)': 'turnover_rate'
    }

    # 重命名列
    renamed_columns = {col: cn_to_en_map.get(col, col) for col in df.columns}
    df = df.rename(columns=renamed_columns)

    # 确保所有必需的列存在
    required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount',
                        'stock_code', 'stock_name', 'market']

    # 添加缺失的列并设置为NaN
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA

    # 确保日期格式正确
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # 只保留需要的列并按日期排序
    df = df[required_columns].sort_values('date')

    return df