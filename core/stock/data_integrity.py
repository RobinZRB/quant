import datetime
import json
import os
import time

import baostock as bs
import pandas as pd

from common.logger import create_log
from settings import stock_data_root, project_root

logger = create_log('data_integrity')

CONFIG_DIR = os.path.join(project_root, 'config')
CONFIG_FILE = os.path.join(CONFIG_DIR, 'stock_data_integrity.json')


def _ensure_config_dir():
    os.makedirs(CONFIG_DIR, exist_ok=True)


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        return _empty_config()
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取配置文件失败: {e}")
        return _empty_config()


def save_config(config: dict):
    _ensure_config_dir()
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def _empty_config() -> dict:
    return {
        'last_check_time': '',
        'target_start_date': '',
        'target_end_date': '',
        'data_source': 'baostock',
        'stocks': {},
        'summary': {
            'total_scanned': 0,
            'complete': 0,
            'incomplete': 0,
            'no_data': 0,
        },
        'download_state': {
            'last_task_id': None,
            'total_to_download': 0,
            'downloaded_count': 0,
            'is_running': False,
        }
    }


def _read_csv_dates(csv_path: str, include_date_set: bool = False) -> tuple:
    try:
        df = pd.read_csv(csv_path, parse_dates=['date'], encoding='utf-8-sig')
        first_date = df['date'].iloc[0]
        last_date = df['date'].iloc[-1]
        total_days = len(df)
        date_set = set(d.date() for d in df['date']) if include_date_set else None

        return str(first_date.date()) if hasattr(first_date, 'date') else str(first_date)[:10], \
               str(last_date.date()) if hasattr(last_date, 'date') else str(last_date)[:10], \
               total_days, \
               date_set
    except Exception as e:
        return None, None, 0, None


def _parse_extracted_date(s: str) -> datetime.date:
    for fmt in ['%Y-%m-%d', '%Y%m%d']:
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None


def _get_trading_days(start_date: datetime.date, end_date: datetime.date) -> set:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"baostock 登录失败，无法获取交易日历: {lg.error_msg}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            rs = bs.query_trade_dates(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'))
            trading_days = set()
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                if row[1] == '1':
                    trading_days.add(datetime.datetime.strptime(row[0], '%Y-%m-%d').date())
            bs.logout()
            return trading_days
        except Exception as e:
            logger.warning(f"获取交易日历失败(第{attempt+1}次): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return None
    return None


def _is_before_market_close() -> bool:
    return datetime.datetime.now().hour < 15


def scan_csv_files(data_source: str = 'baostock', include_date_set: bool = False) -> list[dict]:
    source_dir = os.path.join(stock_data_root, data_source)
    if not os.path.exists(source_dir):
        return []

    results = []
    for fname in os.listdir(source_dir):
        if not fname.endswith('.csv'):
            continue
        csv_path = os.path.join(source_dir, fname)
        first, last, days, date_set = _read_csv_dates(csv_path, include_date_set)

        parts = fname.replace('.csv', '').split('_')
        stock_code = parts[0] if parts else fname

        item = {
            'stock_code': stock_code,
            'csv_name': fname,
            'data_start': first,
            'data_end': last,
            'data_days': days,
        }
        if include_date_set:
            item['date_set'] = date_set

        results.append(item)

    logger.info(f"扫描 {data_source} 目录: 共 {len(results)} 个CSV文件")
    return results


def check_integrity(
    data_source: str = 'baostock',
    target_start: str = None,
    target_end: str = None,
) -> dict:
    if target_start is None:
        target_start = (datetime.datetime.now() - datetime.timedelta(days=365 * 4)).strftime('%Y-%m-%d')
    if target_end is None:
        target_end = datetime.datetime.now().strftime('%Y-%m-%d')

    target_start_dt = _parse_extracted_date(target_start)
    target_end_dt = _parse_extracted_date(target_end)

    adjusted_target_end_dt = target_end_dt
    adjusted_target_end = target_end
    trading_days = None
    if target_start_dt and target_end_dt:
        trading_days = _get_trading_days(target_start_dt, target_end_dt)
        if trading_days:
            if _is_before_market_close():
                today = datetime.date.today()
                trading_days.discard(today)
            if trading_days:
                adjusted_target_end_dt = max(trading_days)
                adjusted_target_end = adjusted_target_end_dt.strftime('%Y-%m-%d')
        else:
            logger.warning("交易日历API不可用，使用周末过滤作为降级方案")
            dt = adjusted_target_end_dt
            while dt.weekday() >= 5:
                dt -= datetime.timedelta(days=1)
            if _is_before_market_close() and dt == datetime.date.today():
                dt -= datetime.timedelta(days=1)
                while dt.weekday() >= 5:
                    dt -= datetime.timedelta(days=1)
            adjusted_target_end_dt = dt
            adjusted_target_end = dt.strftime('%Y-%m-%d')

    existing = scan_csv_files(data_source, include_date_set=True)

    config = _empty_config()
    config['last_check_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    config['target_start_date'] = target_start
    config['target_end_date'] = adjusted_target_end
    config['data_source'] = data_source

    complete = 0
    incomplete = 0
    no_data = 0

    for item in existing:
        code = item['stock_code']
        data_start = item['data_start']
        data_end = item['data_end']
        date_set = item.get('date_set')

        data_start_dt = _parse_extracted_date(data_start) if data_start else None
        data_end_dt = _parse_extracted_date(data_end) if data_end else None

        coverage_ok = False
        is_complete = False
        covered_count = 0
        missing_dates = []

        if data_start_dt and data_end_dt and target_start_dt and adjusted_target_end_dt:
            effective_start = max(data_start_dt, target_start_dt)

            reaches_end = data_end_dt >= adjusted_target_end_dt

            if trading_days:
                effective_trading_days = {d for d in trading_days
                                          if effective_start <= d <= adjusted_target_end_dt}
                effective_expected_trading = len(effective_trading_days)

                if date_set:
                    covered_set = date_set & effective_trading_days
                    covered_count = len(covered_set)
                    missing_set = effective_trading_days - covered_set
                    missing_dates = sorted(d.strftime('%Y-%m-%d') for d in missing_set)
                else:
                    covered_count = 0
            else:
                effective_calendar_days = max((adjusted_target_end_dt - effective_start).days + 1, 1)
                effective_expected_trading = int(effective_calendar_days * 0.69)
                covered_count = item['data_days']

            coverage_ok = data_start_dt <= target_start_dt and data_end_dt >= adjusted_target_end_dt

            if not coverage_ok and reaches_end and not missing_dates:
                coverage_ok = True

            is_complete = reaches_end and covered_count >= int(effective_expected_trading * 0.95)

        config['stocks'][code] = {
            'csv_name': item['csv_name'],
            'data_start': data_start,
            'data_end': data_end,
            'data_days': item['data_days'],
            'in_range': coverage_ok,
            'is_complete': is_complete,
            'missing_dates': missing_dates,
        }

        if is_complete:
            complete += 1
        else:
            incomplete += 1

    config['summary'] = {
        'total_scanned': len(existing),
        'complete': complete,
        'incomplete': incomplete,
        'no_data': no_data,
    }

    save_config(config)
    logger.info(f"完整性检查完成: 完整={complete}, 不完整={incomplete}, 总计={len(existing)}")
    return config


def _find_contiguous_ranges(dates: list) -> list:
    if not dates:
        return []
    sorted_dates = sorted(_parse_extracted_date(d) for d in dates)
    ranges = []
    start = prev = sorted_dates[0]
    for d in sorted_dates[1:]:
        if (d - prev).days == 1:
            prev = d
        else:
            ranges.append((start.strftime('%Y-%m-%d'), prev.strftime('%Y-%m-%d')))
            start = prev = d
    ranges.append((start.strftime('%Y-%m-%d'), prev.strftime('%Y-%m-%d')))
    return ranges


def get_incomplete_stocks_for_download(
    config: dict,
    user_stock_configs: list[dict],
    data_source: str = 'baostock',
) -> dict:
    stocks_map = config.get('stocks', {})
    user_map = {s['stock_code']: s for s in user_stock_configs}

    full = []
    incremental = []
    skipped = 0

    for s in user_stock_configs:
        code = s['stock_code']
        msg = stocks_map.get(code)

        if msg is None:
            full.append(s)
            continue

        if msg.get('is_complete', False):
            skipped += 1
            continue

        missing_strs = msg.get('missing_dates', [])
        if not missing_strs:
            full.append(s)
            continue

        missing_ranges = _find_contiguous_ranges(missing_strs)
        incremental.append({
            'stock_code': code,
            'csv_name': msg.get('csv_name', ''),
            'missing_ranges': missing_ranges,
            'data_source': s.get('data_source', ''),
            'market': s.get('market', ''),
            'stock_name': s.get('stock_name', ''),
        })

    logger.info(f"下载计划: 全量={len(full)}, 增量={len(incremental)}, 跳过={skipped}")
    return {
        'full': full,
        'incremental': incremental,
        'skipped': skipped,
    }


def get_stocks_to_download(config: dict, all_stock_configs: list[dict]) -> list[dict]:
    stocks_map = config.get('stocks', {})

    to_download = []
    skipped = 0

    for s in all_stock_configs:
        code = s['stock_code']
        existing = stocks_map.get(code)
        if existing and existing.get('is_complete', False):
            skipped += 1
            continue
        to_download.append(s)

    logger.info(f"去重: {len(all_stock_configs)} 只 → 需下载 {len(to_download)} 只 (跳过 {skipped} 只已完成)")
    return to_download


def update_config_after_download(config: dict, stock_code: str, csv_name: str = None, source_dir: str = 'baostock'):
    if csv_name:
        csv_path = os.path.join(stock_data_root, source_dir, csv_name)
    else:
        csv_path = os.path.join(stock_data_root, source_dir, f"{stock_code}_*.csv")
        import glob
        matches = glob.glob(csv_path)
        if not matches:
            return config
        csv_path = matches[0]
        csv_name = os.path.basename(csv_path)

    first, last, days, date_set = _read_csv_dates(csv_path)

    target_start_dt = _parse_extracted_date(config.get('target_start_date', ''))
    target_end_dt = _parse_extracted_date(config.get('target_end_date', ''))
    data_start_dt = _parse_extracted_date(first) if first else None
    data_end_dt = _parse_extracted_date(last) if last else None

    adjusted_target_end_dt = target_end_dt

    coverage_ok = False
    is_complete = False
    missing_dates = []

    if data_start_dt and data_end_dt and target_start_dt and adjusted_target_end_dt:
        effective_start = max(data_start_dt, target_start_dt)
        reaches_end = data_end_dt >= adjusted_target_end_dt
        coverage_ok = data_start_dt <= target_start_dt and data_end_dt >= adjusted_target_end_dt

        effective_calendar_days = max((adjusted_target_end_dt - effective_start).days + 1, 1)
        effective_expected_trading = int(effective_calendar_days * 0.69)
        covered_count = days

        is_complete = reaches_end and covered_count >= int(effective_expected_trading * 0.95)

    config['stocks'][stock_code] = {
        'csv_name': csv_name,
        'data_start': first,
        'data_end': last,
        'data_days': days,
        'in_range': coverage_ok,
        'is_complete': is_complete,
        'missing_dates': missing_dates,
    }

    return config


def recount_summary(config: dict):
    complete = sum(1 for s in config['stocks'].values() if s.get('is_complete'))
    incomplete = len(config['stocks']) - complete
    config['summary'] = {
        'total_scanned': len(config['stocks']),
        'complete': complete,
        'incomplete': incomplete,
        'no_data': 0,
    }
    return config


def sort_csv_by_date(csv_path: str):
    try:
        df = pd.read_csv(csv_path, parse_dates=['date'], encoding='utf-8-sig')
        if 'date' not in df.columns:
            return
        df = df.sort_values('date').reset_index(drop=True)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    except Exception as e:
        logger.warning(f"排序CSV失败 {csv_path}: {e}")
