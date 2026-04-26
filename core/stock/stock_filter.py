import baostock as bs
import pandas as pd

from common.logger import create_log

logger = create_log('stock_filter')

EXCLUDE_PREFIX_DEFAULT = ['3']


def get_a_stock_list() -> pd.DataFrame:
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"baostock登录失败: {lg.error_msg}")
        return pd.DataFrame()

    try:
        rs = bs.query_stock_basic(code_name="")
        if rs.error_code != '0':
            logger.error(f"查询股票列表失败: {rs.error_msg}")
            return pd.DataFrame()

        data_list = []
        while (rs.error_code == '0') and rs.next():
            data_list.append(rs.get_row_data())

        columns = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
        df = pd.DataFrame(data_list, columns=columns)
        logger.info(f"获取到全量A股 {len(df)} 只")
        return df
    except Exception as e:
        logger.error(f"获取A股列表失败: {e}")
        return pd.DataFrame()
    finally:
        bs.logout()


def filter_stocks(
    df: pd.DataFrame,
    exclude_indices: bool = True,
    exclude_delisted: bool = True,
    exclude_st: bool = True,
    exclude_prefixes: list = None,
    include_prefixes: list = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()

    if exclude_indices:
        result = result[result['type'] == '1']
        logger.info(f"排除指数/ETF后: {len(result)} 只")

    if exclude_delisted:
        result = result[result['status'] == '1']
        result = result[result['outDate'].isna() | (result['outDate'] == '')]
        logger.info(f"排除退市后: {len(result)} 只")

    if exclude_st:
        st_mask = result['code_name'].str.contains(r'\*?ST', na=False, regex=True)
        result = result[~st_mask]
        logger.info(f"排除ST后: {len(result)} 只")

    if exclude_prefixes:
        prefix_list = exclude_prefixes if exclude_prefixes else []
        for prefix in prefix_list:
            mask = result['code'].str.extract(r'\.(\d{1,2})', expand=False)
            result = result[~mask.str.startswith(prefix, na=False)]
        logger.info(f"排除前缀 {prefix_list} 后: {len(result)} 只")

    if include_prefixes:
        prefix_list = include_prefixes if include_prefixes else []
        combined_mask = pd.Series(False, index=result.index)
        for prefix in prefix_list:
            mask = result['code'].str.extract(r'\.(\d{1,2})', expand=False)
            combined_mask = combined_mask | mask.str.startswith(prefix, na=False)
        result = result[combined_mask]
        logger.info(f"仅保留前缀 {prefix_list} 后: {len(result)} 只")

    return result


def get_filtered_stock_configs(
    exclude_indices: bool = True,
    exclude_delisted: bool = True,
    exclude_st: bool = True,
    exclude_prefixes: list = None,
    include_prefixes: list = None,
    adjust_type: str = '2',
) -> list[dict]:
    df = get_a_stock_list()
    if df.empty:
        return []

    filtered = filter_stocks(df, exclude_indices, exclude_delisted, exclude_st, exclude_prefixes, include_prefixes)

    configs = []
    for _, row in filtered.iterrows():
        configs.append({
            "market": "cn",
            "data_source": "baostock",
            "stock_code": row['code'],
            "stock_name": row['code_name'],
            "adjust_type": adjust_type,
        })

    logger.info(f"生成 {len(configs)} 条股票下载配置")
    return configs
