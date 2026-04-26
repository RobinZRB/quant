import pandas as pd


def get_sample_signal_records():
    """
    获取示例信号记录

    返回:
        信号记录DataFrame
    """
    signal_records = pd.DataFrame([
        {'date': '2000-01-15', 'signal_type': 'normal_buy', 'signal_description': '多'},  # 仅信号，未操作
        {'date': '2000-01-17', 'signal_type': 'normal_buy', 'signal_description': '多(执行)'},  # 信号+操作
        {'date': '2000-02-18', 'signal_type': 'strong_buy', 'signal_description': '强多'},  # 仅信号，未操作
        {'date': '2000-02-20', 'signal_type': 'strong_buy', 'signal_description': '强多(执行)'},  # 信号+操作
        {'date': '2000-03-18', 'signal_type': 'normal_sell', 'signal_description': '空'},  # 仅信号，未操作
        {'date': '2000-03-20', 'signal_type': 'normal_sell', 'signal_description': '空(执行)'},  # 信号+操作
        {'date': '2000-04-13', 'signal_type': 'strong_sell', 'signal_description': '强空'},  # 仅信号，未操作
        {'date': '2000-04-15', 'signal_type': 'strong_sell', 'signal_description': '强空(执行)'},  # 信号+操作
        {'date': '2000-05-06', 'signal_type': 'normal_buy', 'signal_description': '多'},  # 仅信号，未操作
        {'date': '2000-05-08', 'signal_type': 'normal_buy', 'signal_description': '多(执行)'},  # 信号+操作
        {'date': '2000-06-10', 'signal_type': 'strong_buy', 'signal_description': '强多'},  # 仅信号，未操作
        {'date': '2000-06-12', 'signal_type': 'strong_buy', 'signal_description': '强多(执行)'},  # 信号+操作
        {'date': '2000-07-28', 'signal_type': 'normal_sell', 'signal_description': '空'},  # 仅信号，未操作
        {'date': '2000-07-30', 'signal_type': 'normal_sell', 'signal_description': '空(执行)'},  # 信号+操作
        {'date': '2000-08-23', 'signal_type': 'strong_sell', 'signal_description': '强空'},  # 仅信号，未操作
        {'date': '2000-08-25', 'signal_type': 'strong_sell', 'signal_description': '强空(执行)'},  # 信号+操作
    ])
    # 将日期转换为datetime类型
    signal_records['date'] = pd.to_datetime(signal_records['date'])
    return signal_records

def get_sample_trade_records():
    """
    获取示例交易记录

    返回:
        交易记录DataFrame
    """
    trade_records = pd.DataFrame([
        {'date': '2000-01-17', 'action': 'B', 'signal_type': 'normal_buy', 'shares': 200},
        {'date': '2000-02-20', 'action': 'B', 'signal_type': 'strong_buy', 'shares': 300},
        {'date': '2000-03-20', 'action': 'S', 'signal_type': 'normal_sell', 'shares': 200},
        {'date': '2000-04-15', 'action': 'S', 'signal_type': 'strong_sell', 'shares': 100},
        {'date': '2000-05-08', 'action': 'B', 'signal_type': 'normal_buy', 'shares': 200},
        {'date': '2000-06-12', 'action': 'B', 'signal_type': 'strong_buy', 'shares': 250},
        {'date': '2000-07-30', 'action': 'S', 'signal_type': 'normal_sell', 'shares': 200},
        {'date': '2000-08-25', 'action': 'S', 'signal_type': 'strong_sell', 'shares': 250},
    ])
    # 将日期转换为datetime类型
    trade_records['date'] = pd.to_datetime(trade_records['date'])
    return trade_records

def get_sample_asset_records():
    asset_records = pd.DataFrame([
        {'date': '2000-01-15', 'total_assets': 1000000},  # 初始资金
        {'date': '2000-01-17', 'total_assets': 799800},
        {'date': '2000-02-20', 'total_assets': 409400},
        {'date': '2000-03-20', 'total_assets': 1200200},
        {'date': '2000-04-15', 'total_assets': 2000300}
    ])
    # 将日期转换为datetime类型
    asset_records['date'] = pd.to_datetime(asset_records['date'])
    return asset_records