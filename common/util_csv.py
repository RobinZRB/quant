import os
from common.logger import create_log
import pandas as pd

logger = create_log("util_csv")


def save_to_csv(df, filename):
    """将数据保存到CSV文件"""
    if not df.empty:
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # 保存到CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"数据已保存到 {filename}")
    else:
        logger.warning("无数据可保存")

def load_stock_data(csv_path):
    """
    加载股票数据并设置日期索引

    参数:
        csv_path: 股票数据CSV文件路径

    返回:
        加载好的DataFrame
    """
    df = pd.read_csv(
        csv_path,
        parse_dates=['date'],  # 解析date列为datetime类型
        index_col='date'  # 将date列设为索引，方便按日期查询
    )
    return df

def read_data(csv_path):
    """
    读取信号数据并设置日期索引

    参数:
        csv_path: 信号数据CSV文件路径

    返回:
        加载好的DataFrame
    """
    df = pd.read_csv(
        csv_path,
    )
    return df

def combine_data(data_list, ignore_index=True):
    combined_df = pd.concat(data_list, ignore_index=ignore_index)
    return combined_df

