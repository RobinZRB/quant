import concurrent.futures
import datetime
import json
import os
import threading
import time
import uuid

from common.logger import create_log
from core.stock.data_integrity import load_config, save_config, update_config_after_download, recount_summary, sort_csv_by_date, get_incomplete_stocks_for_download
from settings import stock_data_root

logger = create_log('manager_batch')

PROGRESS_DIR = os.path.join(stock_data_root, '.download_progress')
CONFIG_UPDATE_INTERVAL = 10  # 每下载完N只股票，同步一次config


def _ensure_progress_dir():
    os.makedirs(PROGRESS_DIR, exist_ok=True)


def _get_progress_file(task_id):
    return os.path.join(PROGRESS_DIR, f"{task_id}.json")


def _save_progress(task_id, data):
    _ensure_progress_dir()
    data['last_update'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(_get_progress_file(task_id), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_progress(task_id):
    path = _get_progress_file(task_id)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def cancel_download(task_id: str) -> bool:
    progress = _load_progress(task_id)
    if progress is None:
        return False
    progress['cancelled'] = True
    _save_progress(task_id, progress)
    logger.info(f"设置取消标志: {task_id}")

    # 同步config
    _sync_config_from_progress(progress)

    # 也检查子任务
    for suffix in ['_cn', '_other']:
        sub = _load_progress(f"{task_id}{suffix}")
        if sub:
            sub['cancelled'] = True
            _save_progress(f"{task_id}{suffix}", sub)
            _sync_config_from_progress(sub)
    return True


def _sync_config_from_progress(progress: dict):
    try:
        config = load_config()
        results = progress.get('results', [])
        for r in results:
            if r.get('success') and r.get('csv_name'):
                update_config_after_download(config, r['stock_code'], r['csv_name'])
        config = recount_summary(config)
        config['download_state']['last_task_id'] = progress.get('task_id', '')
        config['download_state']['is_running'] = progress.get('status') == 'running'
        save_config(config)
        logger.info(f"config已同步: {len(results)} 条记录")
    except Exception as e:
        logger.warning(f"同步config失败: {e}")


def _is_cancelled(task_id: str) -> bool:
    progress = _load_progress(task_id)
    if progress and progress.get('cancelled', False):
        return True
    return False


def _download_baostock_batch(task_id, full_configs, incremental_configs, start_date, end_date):
    import core.stock.manager_baostock as manager_baostock

    total_count = len(full_configs) + len(incremental_configs)
    progress = _load_progress(task_id)
    if progress is None:
        progress = {
            'task_id': task_id,
            'total': total_count,
            'completed': 0,
            'success': 0,
            'failed': 0,
            'current_stock': '',
            'results': [],
            'status': 'running',
            'cancelled': False,
            'start_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        _save_progress(task_id, progress)

    if _is_cancelled(task_id):
        progress['status'] = 'cancelled'
        _save_progress(task_id, progress)
        return

    completed_codes = {r['stock_code'] for r in progress.get('results', [])}
    results_list = list(progress.get('results', []))

    CHUNK_SIZE = 30

    # 1. 全量下载
    if full_configs:
        pending_full = [s for s in full_configs if s['stock_code'] not in completed_codes]
        for chunk_start in range(0, len(pending_full), CHUNK_SIZE):
            if _is_cancelled(task_id):
                progress['status'] = 'cancelled'
                _save_progress(task_id, progress)
                _sync_config_from_progress(progress)
                return

            chunk = pending_full[chunk_start:chunk_start + CHUNK_SIZE]
            chunk_codes = [s['stock_code'] for s in chunk]
            logger.info(f"全量下载分块 {chunk_start // CHUNK_SIZE + 1}: {len(chunk_codes)} 只")

            try:
                batch_results = manager_baostock.batch_get_cn_stock_history(
                    chunk_codes, start_date, end_date, adjust_type='2', output_dir='baostock'
                )
            except Exception as e:
                logger.error(f"分块下载异常: {e}, 重试")
                try:
                    batch_results = manager_baostock.batch_get_cn_stock_history(
                        chunk_codes, start_date, end_date, adjust_type='2', output_dir='baostock'
                    )
                except Exception as e2:
                    logger.error(f"重试也失败: {e2}")
                    for code in chunk_codes:
                        progress['completed'] += 1
                        progress['failed'] += 1
                        results_list.append({
                            'stock_code': code, 'stock_name': '', 'market': 'cn',
                            'data_source': 'baostock', 'success': False,
                            'csv_name': None, 'error': str(e2),
                        })
                    progress['results'] = results_list
                    _save_progress(task_id, progress)
                    if chunk_start % (CHUNK_SIZE * 3) == 0:
                        _sync_config_from_progress(progress)
                    continue

            received = {r[0] for r in batch_results}
            for code, success, csv_name, error in batch_results:
                if _is_cancelled(task_id):
                    progress['status'] = 'cancelled'
                    _save_progress(task_id, progress)
                    _sync_config_from_progress(progress)
                    return

                progress['completed'] += 1
                progress['current_stock'] = code
                if success:
                    progress['success'] += 1
                    try:
                        csv_path = os.path.join(stock_data_root, 'baostock', csv_name)
                        sort_csv_by_date(csv_path)
                    except Exception:
                        pass
                else:
                    progress['failed'] += 1
                results_list.append({
                    'stock_code': code, 'stock_name': '', 'market': 'cn',
                    'data_source': 'baostock', 'success': success,
                    'csv_name': csv_name, 'error': error or '',
                })

            for code in chunk_codes:
                if code not in received:
                    progress['completed'] += 1
                    progress['failed'] += 1
                    results_list.append({
                        'stock_code': code, 'stock_name': '', 'market': 'cn',
                        'data_source': 'baostock', 'success': False,
                        'csv_name': None, 'error': '未在返回结果中',
                    })

            progress['results'] = results_list
            _save_progress(task_id, progress)

            if progress['completed'] % CONFIG_UPDATE_INTERVAL == 0:
                _sync_config_from_progress(progress)

    # 2. 增量下载 (共用一次 baostock 登录)
    if incremental_configs:
        pending_incr = [s for s in incremental_configs if s['stock_code'] not in completed_codes]
        if pending_incr:
            logged_in = manager_baostock.init_baostock()
            try:
                for s in pending_incr:
                    if _is_cancelled(task_id):
                        break

                    code = s['stock_code']
                    csv_name = s['csv_name']
                    missing_ranges = s.get('missing_ranges', [])
                    progress['current_stock'] = code

                    if csv_name and missing_ranges and logged_in:
                        csv_path = os.path.join(stock_data_root, 'baostock', csv_name)
                        success = manager_baostock._merge_into_csv(
                            csv_path, code, missing_ranges
                        )
                        if success:
                            progress['success'] += 1
                            sort_csv_by_date(csv_path)
                        else:
                            progress['failed'] += 1
                    else:
                        success = False

                    progress['completed'] += 1
                    results_list.append({
                        'stock_code': code,
                        'stock_name': s.get('stock_name', ''),
                        'market': s.get('market', 'cn'),
                        'data_source': s.get('data_source', 'baostock'),
                        'success': success,
                        'csv_name': csv_name,
                        'error': '' if success else '增量合并失败',
                    })
                    progress['results'] = results_list
                    _save_progress(task_id, progress)

                    if progress['completed'] % CONFIG_UPDATE_INTERVAL == 0:
                        _sync_config_from_progress(progress)
            finally:
                if logged_in:
                    manager_baostock.bs.logout()
                else:
                    for s in pending_incr:
                        progress['completed'] += 1
                        progress['failed'] += 1
                        results_list.append({
                            'stock_code': s['stock_code'],
                            'stock_name': s.get('stock_name', ''),
                            'market': s.get('market', 'cn'),
                            'data_source': s.get('data_source', 'baostock'),
                            'success': False,
                            'csv_name': s.get('csv_name', ''),
                            'error': 'baostock登录失败',
                        })
                    progress['results'] = results_list
                    _save_progress(task_id, progress)

    if _is_cancelled(task_id):
        progress['status'] = 'cancelled'
    elif progress['completed'] >= progress['total']:
        progress['status'] = 'completed'
    else:
        progress['status'] = 'partial'
    _save_progress(task_id, progress)
    _sync_config_from_progress(progress)


def _download_other_batch(task_id, full_configs, incremental_configs, start_date, end_date, parallel, max_workers):
    import core.stock.manager_akshare as manager_akshare
    import core.stock.manager_futu as manager_futu

    stock_configs = list(full_configs)
    for s in incremental_configs:
        stock_configs.append({
            'stock_code': s['stock_code'],
            'stock_name': s.get('stock_name', ''),
            'market': s.get('market', ''),
            'data_source': s.get('data_source', ''),
        })

    progress = _load_progress(task_id)
    if progress is None:
        progress = {
            'task_id': task_id,
            'total': len(stock_configs),
            'completed': 0,
            'success': 0,
            'failed': 0,
            'current_stock': '',
            'results': [],
            'status': 'running',
            'cancelled': False,
            'start_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        _save_progress(task_id, progress)

    if _is_cancelled(task_id):
        progress['status'] = 'cancelled'
        _save_progress(task_id, progress)
        return

    completed_codes = {r['stock_code'] for r in progress.get('results', [])}
    pending = [s for s in stock_configs if s['stock_code'] not in completed_codes]

    if not pending:
        progress['status'] = 'completed'
        _save_progress(task_id, progress)
        _sync_config_from_progress(progress)
        return

    results_list = list(progress.get('results', []))

    def _download_one(config):
        result = {
            'stock_code': config['stock_code'],
            'stock_name': config.get('stock_name', ''),
            'market': config.get('market', ''),
            'data_source': config.get('data_source', ''),
            'success': False,
            'csv_name': None,
            'error': '',
        }
        try:
            data_source = config.get('data_source', '')
            market = config.get('market', '')
            clean_code = config['stock_code'].replace('HK.', '').replace('US.', '')
            adjust = config.get('adjust_type', 'qfq')

            if data_source == 'akshare' and market == 'hk':
                success, csv_name = manager_akshare.get_single_hk_stock_history(
                    clean_code, start_date, end_date, adjust, output_dir=data_source)
            elif data_source == 'akshare' and market == 'us':
                success, csv_name = manager_akshare.get_single_us_history(
                    clean_code, start_date, end_date, output_dir=data_source)
            elif data_source == 'futu' and market == 'hk':
                success, csv_name = manager_futu.get_single_hk_stock_history(
                    config['stock_code'], start_date, end_date, adjust, output_dir=data_source)
            elif data_source == 'futu' and market == 'cn':
                success, csv_name = manager_futu.get_single_cn_stock_history(
                    config['stock_code'], start_date, end_date, adjust, output_dir=data_source)
            else:
                result['error'] = f"不支持: data_source={data_source}, market={market}"
                return result

            result['success'] = success
            result['csv_name'] = csv_name
            if not success:
                result['error'] = '下载失败'
        except Exception as e:
            result['error'] = str(e)
        return result

    def _process_result(r):
        if _is_cancelled(task_id):
            return False
        progress['completed'] += 1
        progress['current_stock'] = r['stock_code']
        if r['success']:
            progress['success'] += 1
        else:
            progress['failed'] += 1
        results_list.append(r)
        progress['results'] = results_list
        _save_progress(task_id, progress)
        if progress['completed'] % CONFIG_UPDATE_INTERVAL == 0:
            _sync_config_from_progress(progress)
        time.sleep(0.3)
        return True

    if parallel and len(pending) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_download_one, s): s for s in pending}
            for future in concurrent.futures.as_completed(futures):
                r = future.result()
                if not _process_result(r):
                    break
    else:
        for s in pending:
            if _is_cancelled(task_id):
                break
            r = _download_one(s)
            if not _process_result(r):
                break

    if _is_cancelled(task_id):
        progress['status'] = 'cancelled'
    elif progress['completed'] >= progress['total']:
        progress['status'] = 'completed'
    else:
        progress['status'] = 'partial'
    _save_progress(task_id, progress)
    _sync_config_from_progress(progress)


def start_batch_download(
    stock_configs: list[dict],
    start_date: str = None,
    end_date: str = None,
    parallel: bool = False,
    max_workers: int = 4,
    resume_task_id: str = None,
    use_incremental: bool = True,
) -> str:
    if start_date is None:
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 4)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")

    config = load_config()
    if use_incremental and config.get('stocks'):
        plan = get_incomplete_stocks_for_download(config, stock_configs)
        full_configs = plan['full']
        incremental_configs = plan['incremental']
        skipped = plan['skipped']
        total_pending = len(full_configs) + len(incremental_configs)
        logger.info(f"增量下载计划: 全量={len(full_configs)}, 增量={len(incremental_configs)}, 跳过={skipped}")
    else:
        full_configs = stock_configs
        incremental_configs = []
        total_pending = len(stock_configs)
        logger.info(f"全量下载模式: {total_pending} 只")

    if total_pending == 0:
        task_id = f"dl_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        progress = {
            'task_id': task_id,
            'total': 0,
            'completed': 0,
            'success': 0,
            'failed': 0,
            'current_stock': '',
            'results': [],
            'status': 'completed',
            'cancelled': False,
            'start_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        _save_progress(task_id, progress)
        return task_id

    if resume_task_id:
        existing = _load_progress(resume_task_id)
        if existing and existing.get('status') in ('running', 'partial'):
            reloaded = _load_progress(resume_task_id)
            if reloaded and reloaded.get('cancelled'):
                resume_task_id = None
            else:
                task_id = resume_task_id
        else:
            resume_task_id = None

    if not resume_task_id:
        task_id = f"dl_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    config = load_config()
    config['download_state']['last_task_id'] = task_id + '_cn'
    config['download_state']['total_to_download'] = total_pending
    config['download_state']['downloaded_count'] = 0
    config['download_state']['is_running'] = True
    save_config(config)

    baostock_full = [s for s in full_configs if s.get('data_source') == 'baostock']
    baostock_incr = [s for s in incremental_configs if s.get('data_source') == 'baostock']
    other_full = [s for s in full_configs if s.get('data_source') != 'baostock']
    other_incr = [s for s in incremental_configs if s.get('data_source') != 'baostock']

    if baostock_full or baostock_incr:
        combined_id = f"{task_id}_cn"
        t = threading.Thread(
            target=_download_baostock_batch,
            args=(combined_id, baostock_full, baostock_incr, start_date, end_date),
            daemon=True
        )
        t.start()

    if other_full or other_incr:
        other_id = f"{task_id}_other"
        t = threading.Thread(
            target=_download_other_batch,
            args=(other_id, other_full, other_incr, start_date, end_date, parallel, max_workers),
            daemon=True
        )
        t.start()

    return task_id


def get_download_progress(task_id: str) -> dict:
    cn = _load_progress(f"{task_id}_cn")
    other = _load_progress(f"{task_id}_other")

    if cn is None and other is None:
        cn = _load_progress(task_id)
        if cn is None:
            return {'task_id': task_id, 'status': 'not_found', 'total': 0, 'completed': 0, 'success': 0, 'failed': 0, 'current_stock': '', 'results': []}

    merged = {
        'task_id': task_id,
        'status': 'running',
        'total': 0,
        'completed': 0,
        'success': 0,
        'failed': 0,
        'current_stock': '',
        'results': [],
    }

    for p in [cn, other]:
        if p is None:
            continue
        merged['total'] += p.get('total', 0)
        merged['completed'] += p.get('completed', 0)
        merged['success'] += p.get('success', 0)
        merged['failed'] += p.get('failed', 0)
        if p.get('current_stock'):
            merged['current_stock'] = p['current_stock']
        merged['results'].extend(p.get('results', []))

    # merge status: cancelled has priority
    statuses = set()
    for p in [cn, other]:
        if p:
            statuses.add(p.get('status', 'not_found'))
    if 'cancelled' in statuses:
        merged['status'] = 'cancelled'
    elif 'running' in statuses:
        merged['status'] = 'running'
    elif 'completed' in statuses and len(statuses) == 1:
        merged['status'] = 'completed'
    elif cn is None and other is None:
        merged['status'] = 'not_found'
    else:
        merged['status'] = 'running'

    return merged


def batch_download_stocks(
    stock_configs: list[dict],
    start_date: str = None,
    end_date: str = None,
    parallel: bool = False,
    max_workers: int = 4,
    sleep_interval: float = 0.3,
) -> list[dict]:
    """
    同步批量下载（阻塞等待完成），用于非前端调用（如CLI）。
    """
    if start_date is None:
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * 4)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")

    import core.stock.manager_baostock as manager_baostock

    baostock_configs = [s for s in stock_configs if s.get('data_source') == 'baostock']
    other_configs = [s for s in stock_configs if s.get('data_source') != 'baostock']

    results = []

    if baostock_configs:
        codes = [s['stock_code'] for s in baostock_configs]
        baostock_results = manager_baostock.batch_get_cn_stock_history(codes, start_date, end_date, output_dir='baostock')
        for code, success, csv_name, error in baostock_results:
            results.append({
                'stock_code': code,
                'stock_name': '',
                'market': 'cn',
                'data_source': 'baostock',
                'success': success,
                'csv_name': csv_name,
                'error': error or '',
            })

    for config in other_configs:
        r = _download_single_sync(config, start_date, end_date)
        results.append(r)
        time.sleep(sleep_interval)

    return results


def _download_single_sync(config, start_date, end_date):
    import core.stock.manager_akshare as manager_akshare
    import core.stock.manager_futu as manager_futu

    result = {
        'stock_code': config.get('stock_code', ''),
        'stock_name': config.get('stock_name', ''),
        'market': config.get('market', ''),
        'data_source': config.get('data_source', ''),
        'success': False,
        'csv_name': None,
        'error': '',
    }
    try:
        data_source = config.get('data_source', '')
        market = config.get('market', '')
        clean_code = config['stock_code'].replace('HK.', '').replace('US.', '')
        adjust = config.get('adjust_type', 'qfq')

        if data_source == 'akshare' and market == 'hk':
            success, csv_name = manager_akshare.get_single_hk_stock_history(
                clean_code, start_date, end_date, adjust, output_dir=data_source)
        elif data_source == 'akshare' and market == 'us':
            success, csv_name = manager_akshare.get_single_us_history(
                clean_code, start_date, end_date, output_dir=data_source)
        elif data_source == 'futu' and market == 'hk':
            success, csv_name = manager_futu.get_single_hk_stock_history(
                config['stock_code'], start_date, end_date, adjust, output_dir=data_source)
        elif data_source == 'futu' and market == 'cn':
            success, csv_name = manager_futu.get_single_cn_stock_history(
                config['stock_code'], start_date, end_date, adjust, output_dir=data_source)
        else:
            result['error'] = f"不支持: data_source={data_source}, market={market}"
            return result

        result['success'] = success
        result['csv_name'] = csv_name
        if not success:
            result['error'] = '下载失败'
    except Exception as e:
        result['error'] = str(e)
    return result
