import os
import sys


# 首先设置系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
import multiprocessing

import secrets
import shutil
import threading
from datetime import datetime
from functools import wraps
from pathlib import Path
from core.ai.ai_manager import AIManager

from core.signal.signal_handler import signal_get, signals_analyze
from core.task.task_timer import schedule_tasks
from core.strategy.indicator_manager import global_indicator_manager
from core.task.task_manager import TaskManager
from core.task.task_execution_manager import task_execution_manager
from flask import Flask, render_template, request, send_from_directory
from flask_cors import CORS
from flask import make_response
import json
from common.util_csv import combine_data, read_data
from common.util_html import signals_to_html
from core.stock import manager_baostock, manager_akshare, manager_futu
from core.stock.stock_filter import get_filtered_stock_configs
from core.stock.data_integrity import check_integrity as do_check_integrity, load_config as load_integrity_config, get_incomplete_stocks_for_download
from core.stock.manager_batch import start_batch_download, get_download_progress, batch_download_stocks, cancel_download
from core.strategy.strategy_manager import global_strategy_manager
from common.logger import create_log
from core.quant.quant_manage import run_backtest_enhanced_volume_strategy, run_backtest_enhanced_volume_strategy_multi
from settings import stock_data_root, html_root, signals_root, result_root, project_root

# 初始化Flask应用
app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
CORS(app)
logger = create_log('quant_frontend')
task_manager = TaskManager()
# 支持的数据源
DATA_SOURCES = ['akshare', 'baostock', 'futu']

# 批量回测进度管理（参照 manager_batch 的下载进度模式）
_backtest_progress_store = {}
_backtest_cancel_events = {}


def _generate_backtest_task_id():
    import random
    suffix = secrets.token_hex(4)
    return f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{suffix}"


def _run_batch_backtest_thread(task_id, source, strategy_name, init_cash,
                                cut_start, cut_end, resume_old_dir=None):
    """后台线程：执行批量回测并更新进度"""
    try:
        strategy_class = global_strategy_manager.get_strategy(strategy_name)
        if not strategy_class:
            _backtest_progress_store[task_id] = {
                'status': 'failed', 'error': f'无效策略: {strategy_name}'
            }
            return

        # 判断是否 resume
        if resume_old_dir:
            progress_file = Path(resume_old_dir) / "batch_progress.json"
            if progress_file.exists():
                with open(progress_file, 'r', encoding='utf-8') as f:
                    old_progress = json.load(f)
                batch_dir = Path(resume_old_dir)
                all_files = old_progress.get('all_files', [])
                completed_files = {c['file'] for c in old_progress.get('completed', [])}
                failed_files = {f['file'] for f in old_progress.get('failed', [])}
                done_files = completed_files | failed_files
                init_progress = {
                    'task_id': task_id,
                    'status': 'running',
                    'strategy': strategy_name,
                    'source': source,
                    'batch_dir': str(batch_dir),
                    'init_cash': init_cash,
                    'total': len(all_files),
                    'completed_count': len(completed_files),
                    'failed_count': len(failed_files),
                    'current_stock': '',
                    'all_files': all_files,
                    'completed': old_progress.get('completed', []),
                    'failed': old_progress.get('failed', []),
                    'created_at': old_progress.get('created_at', datetime.now().strftime('%Y%m%d_%H%M%S')),
                    'updated_at': datetime.now().strftime('%Y%m%d_%H%M%S'),
                }
                # 重建 all_results 列表
                all_results = []
                for c in old_progress.get('completed', []):
                    r = BacktestResult(csv_path=c.get('file', ''))
                    r.stock_code = c.get('stock_code', '')
                    r.stock_name = c.get('stock_name', '')
                    r.success = True
                    # 尝试从 metrics.json 恢复更多数据
                    metrics_file = batch_dir / f"{c.get('stock_code', '')}_{c.get('stock_name', '')}" / "metrics.json"
                    if metrics_file.exists():
                        try:
                            with open(metrics_file, 'r', encoding='utf-8') as mf:
                                m = json.load(mf)
                            r.total_return_pct = m.get('total_return_pct', 0)
                            r.annual_return_pct = m.get('annual_return_pct', 0)
                            r.max_drawdown_pct = m.get('max_drawdown_pct', 0)
                            r.sharpe_ratio = m.get('sharpe_ratio', 0)
                            r.total_trades = m.get('total_trades', 0)
                            r.won_trades = m.get('won_trades', 0)
                            r.lost_trades = m.get('lost_trades', 0)
                            r.win_rate_pct = m.get('win_rate_pct', 0)
                            r.profit_factor = m.get('profit_factor', 0)
                            r.avg_holding_days = m.get('avg_holding_days', 0)
                            r.data_days = m.get('data_days', 0)
                            r.start_date = m.get('start_date', '')
                            r.end_date = m.get('end_date', '')
                            r.final_cash = m.get('final_cash', 0)
                            r.html_path = m.get('html_path', '')
                        except Exception:
                            pass
                    all_results.append(r)

                folder = Path(source)
                cut_date_range = (cut_start, cut_end) if cut_start or cut_end else None
            else:
                old_progress = None
                batch_dir = None
                all_results = []
                init_progress = None
                folder = Path(source)
                cut_date_range = (cut_start, cut_end) if cut_start or cut_end else None
        else:
            old_progress = None
            batch_dir = None
            all_results = []
            init_progress = None
            folder = Path(source)
            cut_date_range = (cut_start, cut_end) if cut_start or cut_end else None

        cancel_event = threading.Event()
        _backtest_cancel_events[task_id] = cancel_event

        def progress_cb(processed, total, stock_name):
            nonlocal init_progress, batch_dir
            if init_progress is None:
                init_progress = {
                    'task_id': task_id,
                    'status': 'running',
                    'strategy': strategy_name,
                    'source': source,
                    'batch_dir': '',
                    'init_cash': init_cash,
                    'total': total,
                    'completed_count': 0,
                    'failed_count': 0,
                    'current_stock': stock_name,
                    'all_files': [f.name for f in sorted(folder.glob("*.csv"))],
                    'completed': [],
                    'failed': [],
                    'created_at': datetime.now().strftime('%Y%m%d_%H%M%S'),
                    'updated_at': datetime.now().strftime('%Y%m%d_%H%M%S'),
                }
            init_progress['updated_at'] = datetime.now().strftime('%Y%m%d_%H%M%S')
            init_progress['current_stock'] = stock_name
            init_progress['total'] = total
            _backtest_progress_store[task_id] = dict(init_progress)

        run_result = run_backtest_enhanced_volume_strategy_multi(
            str(folder), strategy_class, init_cash,
            cut_date_range=cut_date_range,
            progress_callback=progress_cb,
            cancel_event=cancel_event,
        )
        from core.quant.backtest_result import BacktestResult, aggregate_backtest_results, results_to_csv_data

        result_list, summary, batch_dir_str, cancelled = run_result

        if not resume_old_dir and not old_progress:
            # 首次运行，从回调中获取 init_progress 来完善
            pass

        # 更新 progress 中的 completed/failed 列表
        if init_progress is None:
            total_files = len(list(folder.glob("*.csv")))
            init_progress = {
                'task_id': task_id,
                'status': 'running',
                'strategy': strategy_name,
                'source': source,
                'batch_dir': batch_dir_str,
                'init_cash': init_cash,
                'total': total_files,
                'completed_count': 0,
                'failed_count': 0,
                'current_stock': '',
                'all_files': [f.name for f in sorted(folder.glob("*.csv"))],
                'completed': [],
                'failed': [],
                'created_at': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'updated_at': datetime.now().strftime('%Y%m%d_%H%M%S'),
            }

        init_progress['batch_dir'] = batch_dir_str
        init_progress['completed_count'] = sum(1 for r in result_list if r.success)
        init_progress['failed_count'] = sum(1 for r in result_list if not r.success)

        # 建立 file -> (code, name) 映射
        file_map = {}
        for r in result_list:
            if r.csv_path:
                fname = Path(r.csv_path).name
                file_map[fname] = (r.stock_code, r.stock_name)
            elif r.stock_code:
                file_map[r.stock_code] = (r.stock_code, r.stock_name)

        # 合并已有记录 + 本轮新结果
        if resume_old_dir and old_progress:
            new_completed = []
            new_failed = []
            for r in result_list:
                # 找到对应的文件名
                matched = False
                for af in init_progress['all_files']:
                    if af in r.csv_path or (r.stock_code and r.stock_code in af):
                        entry = {'file': af, 'stock_code': r.stock_code, 'stock_name': r.stock_name}
                        if r.success:
                            new_completed.append(entry)
                        else:
                            entry['error'] = r.error
                            new_failed.append(entry)
                        matched = True
                        break
                if not matched:
                    entry = {'file': r.csv_path, 'stock_code': r.stock_code, 'stock_name': r.stock_name}
                    if r.success:
                        new_completed.append(entry)
                    else:
                        entry['error'] = r.error
                        new_failed.append(entry)
            # 合并：已完成的按 old + new 去重
            old_completed = old_progress.get('completed', [])
            old_failed = old_progress.get('failed', [])
            seen_files = set()
            merged_completed = []
            for e in old_completed + new_completed:
                if e['file'] not in seen_files:
                    seen_files.add(e['file'])
                    merged_completed.append(e)
            seen_failed = set()
            merged_failed = []
            for e in old_failed + new_failed:
                if e['file'] not in seen_failed:
                    seen_failed.add(e['file'])
                    merged_failed.append(e)
            init_progress['completed'] = merged_completed
            init_progress['failed'] = merged_failed
            init_progress['completed_count'] = len(merged_completed)
            init_progress['failed_count'] = len(merged_failed)
        else:
            # 非 resume：从 result_list 构建
            completed_list = []
            failed_list = []
            seen_files = set()
            for r in result_list:
                fname = Path(r.csv_path).name if r.csv_path else r.stock_code
                if fname in seen_files:
                    continue
                seen_files.add(fname)
                entry = {'file': fname, 'stock_code': r.stock_code, 'stock_name': r.stock_name}
                if r.success:
                    completed_list.append(entry)
                else:
                    entry['error'] = r.error
                    failed_list.append(entry)
            init_progress['completed'] = completed_list
            init_progress['failed'] = failed_list

        if cancelled:
            init_progress['status'] = 'cancelled'
        else:
            init_progress['status'] = 'completed'

        init_progress['updated_at'] = datetime.now().strftime('%Y%m%d_%H%M%S')
        _backtest_progress_store[task_id] = dict(init_progress)

        # 持久化进度 config 到 batch 目录
        try:
            progress_dir = Path(batch_dir_str)
            os.makedirs(progress_dir, exist_ok=True)
            with open(progress_dir / "batch_progress.json", 'w', encoding='utf-8') as f:
                json.dump(init_progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存回测进度文件失败: {e}")

        # 存储最终结果以便前端轮询获取
        if not cancelled:
            results_data = []
            for r in result_list:
                results_data.append({
                    'stock_code': r.stock_code,
                    'stock_name': r.stock_name,
                    'market': r.market,
                    'success': r.success,
                    'total_return_pct': round(r.total_return_pct, 2),
                    'annual_return_pct': round(r.annual_return_pct, 2),
                    'max_drawdown_pct': round(r.max_drawdown_pct, 2),
                    'sharpe_ratio': round(r.sharpe_ratio, 2),
                    'total_trades': r.total_trades,
                    'won_trades': r.won_trades,
                    'lost_trades': r.lost_trades,
                    'win_rate_pct': round(r.win_rate_pct, 2),
                    'profit_factor': round(r.profit_factor, 2),
                    'avg_holding_days': round(r.avg_holding_days, 1),
                    'error': r.error,
                })
            init_progress['results'] = results_data
            init_progress['summary'] = summary
            _backtest_progress_store[task_id] = dict(init_progress)

    except Exception as e:
        logger.error(f"批量回测线程异常: {str(e)}", exc_info=True)
        _backtest_progress_store[task_id] = {
            'status': 'failed',
            'error': str(e),
        }
    finally:
        _backtest_cancel_events.pop(task_id, None)


def log_request_details(f):
    """
    记录请求详情的装饰器
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 记录请求开始
        request_id = secrets.token_hex(8)
        start_time = datetime.now()

        # 获取请求详情
        method = request.method
        url = request.url
        path = request.path

        # 获取请求参数
        query_params = dict(request.args)

        # 获取请求体（对于POST请求等）
        request_body = None
        if request.is_json:
            try:
                request_body = request.get_json()
            except Exception as e:
                logger.warning(f"Request ID: {request_id} - Failed to parse JSON body: {str(e)}")

        # 记录请求详情（不记录过大的请求体）
        log_data = {
            'request_id': request_id,
            'method': method,
            'path': path,
            'query_params': query_params,
            'client_ip': request.remote_addr,
            'user_agent': request.headers.get('User-Agent', ''),
        }

        # 只记录较小的请求体，避免日志过大
        if request_body and len(str(request_body)) < 10000:
            log_data['request_body'] = request_body
        elif request_body:
            log_data['request_body_size'] = len(str(request_body))

        logger.info(f"Request received: {json.dumps(log_data, ensure_ascii=False)}")

        try:
            # 执行原始函数
            response = f(*args, **kwargs)

            # 记录响应信息
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds() * 1000  # 毫秒

            # 获取响应状态码
            status_code = response.status_code if hasattr(response, 'status_code') else 200

            logger.info(f"Request completed: request_id={request_id}, status_code={status_code}, "
                        f"processing_time={processing_time:.2f}ms")

            return response

        except Exception as e:
            # 记录异常信息
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds() * 1000  # 毫秒

            logger.error(f"Request failed: request_id={request_id}, error={str(e)}, "
                         f"processing_time={processing_time:.2f}ms", exc_info=True)

            # 重新抛出异常，让Flask处理
            raise

    return decorated_function


@app.route('/')
@log_request_details
def index():
    """主页，显示数据源和股票选择界面"""
    strategies = global_strategy_manager.get_strategy_names()
    return render_template('index.html', data_sources=DATA_SOURCES, strategies=strategies)


@app.route('/get_stocks/<source>')
@log_request_details
def get_stocks(source):
    """获取指定数据源下的所有股票文件"""
    if source not in DATA_SOURCES:
        error_response_data = {'success': False, 'message': f'Invalid data source', 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response

    source_path = stock_data_root / source
    if not os.path.exists(source_path):
        error_response_data = {'success': False, 'message': f'Source directory not found', 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response

    stocks = []
    try:
        for file in os.listdir(source_path):
            if file.endswith('.csv'):
                # 解析文件名获取股票信息
                parts = file.split('_')
                if len(parts) >= 2:
                    stock_code = parts[0]
                    stock_name = parts[1]
                    stocks.append({
                        'file': file,
                        'code': stock_code,
                        'name': stock_name
                    })
    except Exception as e:
        logger.error(f"Error reading stocks: {str(e)}")
        error_response_data = {'success': False, 'message': f'Error reading stocks: {str(e)}', 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response
    response_data = {
        'success': True,
        'message': f'Found {len(stocks)} stocks',
        'data':{
            'stocks': stocks
        }
    }
    response = make_response(json.dumps(response_data, ensure_ascii=False))
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    return response


@app.route('/run_backtest', methods=['POST'])
@log_request_details
def run_backtest():
    """运行回测"""
    try:
        data = request.json
        source = data.get('source')
        stock_file = data.get('stock_file')
        is_batch = data.get('is_batch', False)
        init_cash = float(data.get('init_cash', 5000000))
        strategy_name = data.get('strategy')

        # 验证策略名称
        strategy_class = global_strategy_manager.get_strategy(strategy_name)
        if not strategy_class:
            error_response_data = {'success': False, 'message': f'Invalid strategy: {strategy_name}', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        if source not in DATA_SOURCES:
            error_response_data = {'success': False, 'message': f'Invalid data source', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        if is_batch:
            folder_path = stock_data_root / source
            cut_start = data.get('cut_start', None)
            cut_end = data.get('cut_end', None)
            cut_date_range = (cut_start, cut_end) if cut_start or cut_end else None
            result_list, summary, batch_dir, _ = run_backtest_enhanced_volume_strategy_multi(
                str(folder_path), strategy_class, init_cash, cut_date_range=cut_date_range
            )
            response_data = {
                'success': True,
                'message': 'Batch backtest completed',
                'data': {
                    'results': [{
                        'stock_code': r.stock_code,
                        'stock_name': r.stock_name,
                        'market': r.market,
                        'success': r.success,
                        'total_return_pct': round(r.total_return_pct, 2),
                        'annual_return_pct': round(r.annual_return_pct, 2),
                        'max_drawdown_pct': round(r.max_drawdown_pct, 2),
                        'sharpe_ratio': round(r.sharpe_ratio, 2),
                        'total_trades': r.total_trades,
                        'won_trades': r.won_trades,
                        'lost_trades': r.lost_trades,
                        'win_rate_pct': round(r.win_rate_pct, 2),
                        'profit_factor': round(r.profit_factor, 2),
                        'avg_holding_days': round(r.avg_holding_days, 1),
                        'error': r.error,
                    } for r in result_list],
                    'summary': summary,
                    'batch_dir': batch_dir,
                }
            }
            response = make_response(json.dumps(response_data, ensure_ascii=False))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return response
        else:
            # 单个股票回测
            if not stock_file:
                error_response_data = {'success': False, 'message': f'Stock file is required', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response

            file_path = stock_data_root / source / stock_file
            result = run_backtest_enhanced_volume_strategy(str(file_path), strategy_class, init_cash)

            if result.success and result.html_path:
                relative_path = str(Path(result.html_path).relative_to(project_root))
                response_data = {
                    'success': True,
                    'message': 'Backtest completed',
                    'data': {
                        'result_path': relative_path,
                        'metrics': {
                            'total_return_pct': round(result.total_return_pct, 2),
                            'annual_return_pct': round(result.annual_return_pct, 2),
                            'max_drawdown_pct': round(result.max_drawdown_pct, 2),
                            'sharpe_ratio': round(result.sharpe_ratio, 2),
                            'total_trades': result.total_trades,
                            'win_rate_pct': round(result.win_rate_pct, 2),
                            'profit_factor': round(result.profit_factor, 2),
                        }
                    }
                }
                response = make_response(json.dumps(response_data, ensure_ascii=False))
                response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return response
            else:
                response_data = {'success': False, 'message': result.error or 'Backtest failed', 'data': {}}
                response = make_response(json.dumps(response_data, ensure_ascii=False))
                response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return response
    except Exception as e:
        logger.error(f"回测执行失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'回测执行失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response

@app.route('/run_backtest_async', methods=['POST'])
@log_request_details
def run_backtest_async():
    """异步启动批量回测，立即返回 task_id"""
    try:
        data = request.json
        source = data.get('source')
        is_batch = data.get('is_batch', False)
        init_cash = float(data.get('init_cash', 5000000))
        strategy_name = data.get('strategy')
        cut_start = data.get('cut_start', None)
        cut_end = data.get('cut_end', None)
        resume_task_id = data.get('resume_task_id', None)

        if not is_batch:
            return _json_response({'success': False, 'message': '异步接口仅支持批量回测'})

        strategy_class = global_strategy_manager.get_strategy(strategy_name)
        if not strategy_class:
            return _json_response({'success': False, 'message': f'无效策略: {strategy_name}'})

        if source not in DATA_SOURCES:
            return _json_response({'success': False, 'message': f'无效数据源: {source}'})

        # 检查 resume
        resume_old_dir = None
        if resume_task_id:
            old_progress = _backtest_progress_store.get(resume_task_id)
            if old_progress and old_progress.get('batch_dir'):
                resume_old_dir = old_progress['batch_dir']
            else:
                # 尝试从持久化文件读取
                pass

        task_id = _generate_backtest_task_id()
        logger.info(f"启动异步批量回测: task_id={task_id}, source={source}, strategy={strategy_name}")

        thread = threading.Thread(
            target=_run_batch_backtest_thread,
            args=(task_id, str(stock_data_root / source), strategy_name, init_cash,
                  cut_start, cut_end, resume_old_dir),
            daemon=True,
        )
        thread.start()

        return _json_response({
            'success': True,
            'message': '批量回测已异步启动',
            'data': {'task_id': task_id, 'source': source, 'strategy': strategy_name},
        })
    except Exception as e:
        logger.error(f"启动异步回测失败: {str(e)}")
        return _json_response({'success': False, 'message': f'启动失败: {str(e)}'})


@app.route('/api/backtest_progress/<task_id>', methods=['GET'])
@log_request_details
def get_backtest_progress(task_id):
    """轮询批量回测进度"""
    progress = _backtest_progress_store.get(task_id)
    if not progress:
        return _json_response({'success': False, 'message': '任务不存在或已过期', 'data': {'status': 'not_found'}})
    return _json_response({'success': True, 'message': 'OK', 'data': progress})


@app.route('/api/backtest_cancel/<task_id>', methods=['POST'])
@log_request_details
def cancel_backtest(task_id):
    """取消批量回测"""
    cancel_event = _backtest_cancel_events.get(task_id)
    if cancel_event:
        cancel_event.set()
        logger.info(f"已发送取消信号: task_id={task_id}")
        return _json_response({'success': True, 'message': '已发送取消信号'})
    # 也检查持久化
    progress = _backtest_progress_store.get(task_id)
    if progress and progress.get('status') in ('completed', 'cancelled', 'failed'):
        return _json_response({'success': False, 'message': '任务已结束'})
    return _json_response({'success': False, 'message': '任务不存在或已结束'})


@app.route('/api/backtest_clear', methods=['POST'])
@log_request_details
def clear_backtest_result():
    """清空当前回测结果目录"""
    try:
        data = request.json
        batch_dir = data.get('batch_dir', '')
        task_id = data.get('task_id', '')

        if not batch_dir:
            return _json_response({'success': False, 'message': '缺少 batch_dir'})

        # 先取消（如果还在运行）
        if task_id:
            cancel_event = _backtest_cancel_events.get(task_id)
            if cancel_event:
                cancel_event.set()

        abs_path = Path(batch_dir)
        if abs_path.exists() and abs_path.is_dir():
            shutil.rmtree(abs_path)
            logger.info(f"已清空回测结果目录: {batch_dir}")

        # 清理内存
        if task_id:
            _backtest_progress_store.pop(task_id, None)
            _backtest_cancel_events.pop(task_id, None)

        return _json_response({'success': True, 'message': '已清空回测结果数据'})
    except Exception as e:
        logger.error(f"清空回测结果失败: {str(e)}")
        return _json_response({'success': False, 'message': f'清空失败: {str(e)}'})


def _json_response(data, status_code=200):
    r = make_response(json.dumps(data, ensure_ascii=False))
    r.headers['Content-Type'] = 'application/json; charset=utf-8'
    return r, status_code


@app.route('/api/music/list', methods=['GET'])
@log_request_details
def get_music_list():
    """获取音乐列表"""
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    music_files = []
    if os.path.exists(static_dir):
        for f in os.listdir(static_dir):
            if f.endswith('.mp3'):
                music_files.append({
                    'name': f.replace('.mp3', '').replace('-', ' ').replace('_', ' '),
                    'file': f,
                    'url': f'/static/{f}'
                })
    response_data = {
        'success': True,
        'message': 'Success',
        'data': {'music_list': music_files}
    }
    response = make_response(json.dumps(response_data, ensure_ascii=False))
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    return response


@app.route('/get_backtest_results')
@log_request_details
def get_backtest_results():
    """获取所有回测结果 (results/ 目录结构: strategy/timestamp/stock/chart.html)"""
    results = []
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        start_idx = (page - 1) * page_size

        stock_filter = request.args.get('stock', '')
        date_start = request.args.get('date_start', '')
        date_end = request.args.get('date_end', '')
        strategy_filter = request.args.get('strategy', '')

        if os.path.exists(result_root):
            for strategy_dir in sorted(os.listdir(result_root)):
                if strategy_filter and strategy_dir != strategy_filter:
                    continue
                strategy_path = result_root / strategy_dir
                if not os.path.isdir(strategy_path):
                    continue
                for timestamp_dir in sorted(os.listdir(strategy_path), reverse=True):
                    timestamp_path = strategy_path / timestamp_dir
                    if not os.path.isdir(timestamp_path):
                        continue
                    for stock_dir in sorted(os.listdir(timestamp_path)):
                        if stock_filter and stock_filter.lower() not in stock_dir.lower():
                            continue
                        stock_path = timestamp_path / timestamp_dir
                        chart_file = stock_path / "chart.html"
                        if not chart_file.exists():
                            continue

                        run_time_dt = datetime.fromtimestamp(os.path.getctime(str(chart_file)))
                        run_time = run_time_dt.strftime('%Y-%m-%d %H:%M:%S')
                        run_time_date = run_time.split(' ')[0]

                        if date_start and run_time_date < date_start:
                            continue
                        if date_end and run_time_date > date_end:
                            continue

                        relative_path = f"{strategy_dir}/{timestamp_dir}/{stock_dir}/chart.html"
                        summary_csv_path = timestamp_path / "batch_summary.csv"
                        results.append({
                            'stock': stock_dir,
                            'strategy': strategy_dir,
                            'timestamp': timestamp_dir,
                            'run_time': run_time,
                            'path': relative_path,
                            'has_summary': summary_csv_path.exists(),
                        })

        results.sort(key=lambda x: x['run_time'], reverse=True)
        total = len(results)
        total_pages = (total + page_size - 1) // page_size
        paginated_results = results[start_idx:start_idx + page_size]

        response_data = {
            'success': True,
            'message': f'Found {total} backtest results',
            'data': {
                'results': paginated_results,
                'page': page,
                'total_pages': total_pages,
                'total': total
            }
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response

    except Exception as e:
        logger.error(f"Error getting backtest results: {str(e)}")
        error_response_data = {'success': False, 'message': f'Error getting backtest results: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/show_result/<path:result_path>')
@log_request_details
def show_result(result_path):
    """显示回测结果图表"""
    try:
        actual_path = os.path.join(result_root, result_path)
        if not os.path.exists(actual_path):
            actual_path = os.path.join(html_root, result_path)
        if not os.path.exists(actual_path):
            error_response_data = {'success': False, 'message': 'Result file not found', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        with open(actual_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        return render_template('result_viewer.html', html_content=html_content, file_path=result_path)

    except Exception as e:
        logger.error(f"Error showing result: {str(e)}")
        error_response_data = {'success': False, 'message': f'Error showing result: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/static/<path:filename>')
@log_request_details
def serve_static(filename):
    """提供静态文件服务"""
    return send_from_directory('static', filename)


@app.route('/html/<path:filename>')
@log_request_details
def serve_html(filename):
    """提供HTML结果文件服务"""
    return send_from_directory(str(html_root), filename)


@app.route('/acquire_stock_data', methods=['POST'])
@log_request_details
def acquire_stock_data():
    """获取股票历史数据"""
    try:
        data = request.json
        market = data.get('market')
        data_source = data.get('data_source')
        stock_code = data.get('stock_code')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        adjust_type = data.get('adjust_type', 'qfq')

        # 参数验证
        if not all([market, data_source, stock_code, start_date, end_date]):
            error_response_data = {'success': False, 'message': '缺少必要参数', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        if data_source not in DATA_SOURCES:
            error_response_data = {'success': False, 'message': f'不支持的数据源: {data_source}', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        logger.info(f"开始获取数据: 市场={market}, 数据源={data_source}, 股票代码={stock_code}")

        # 根据市场和数据源调用不同的数据获取函数
        success = False
        filename = None
        if data_source == 'akshare':
            if market == 'hk':
                if not stock_code.startswith('HK') :
                    error_response_data = {'success': False, 'message': f'{market}股票代码请保证前缀HK: {stock_code}', 'data':{}}
                    error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                    error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return error_response
                stock_code = stock_code.replace('HK.', '')
                success, filename = manager_akshare.get_single_hk_stock_history(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_type=adjust_type,
                    output_dir=data_source
                )
            elif market == 'us':
                if not stock_code.startswith('US') :
                    error_response_data = {'success': False, 'message': f'{market}股票代码请保证前缀US: {stock_code}', 'data':{}}
                    error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                    error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return error_response
                stock_code = stock_code.replace('US.', '')
                success, filename = manager_akshare.get_single_us_history(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    output_dir=data_source
                )
            else:
                error_response_data = {'success': False, 'message': f'暂不支持的市场: {market}', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response
        elif data_source == 'baostock':
            if adjust_type == 'qfq':
                adjust_type = '2'
            elif adjust_type == 'hfq':
                adjust_type = '3'
            elif adjust_type == 'bfq':
                adjust_type = '1'
            else:
                error_response_data = {'success': False, 'message': f'不支持的调整类型: {adjust_type}', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response
            if market == 'cn':
                if not stock_code.startswith('SH') and not stock_code.startswith('SZ'):
                    error_response_data = {'success': False, 'message': f'{market}股票代码请保证前缀SH或SZ: {stock_code}', 'data':{}}
                    error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                    error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return error_response
                stock_code = stock_code.replace('SH.', 'sh.')
                stock_code = stock_code.replace('SZ.', 'sz.')
                success, filename = manager_baostock.get_single_cn_stock_history(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_type=adjust_type,
                    output_dir=data_source
                )
            else:
                error_response_data = {'success': False, 'message': f'暂不支持的市场: {market}', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response
        elif data_source == 'futu':
            if adjust_type == 'qfq':
                pass
            elif adjust_type == 'hfq':
                pass
            elif adjust_type == 'bfq':
                adjust_type = 'None'
            else:
                error_response_data = {'success': False, 'message': f'不支持的调整类型: {adjust_type}', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response
            if market == 'cn':
                if not stock_code.startswith('SH') and not stock_code.startswith('SZ'):
                    error_response_data = {'success': False, 'message': f'{market}股票代码请保证前缀SH或SZ: {stock_code}', 'data':{}}
                    error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                    error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return error_response
                success, filename = manager_futu.get_single_cn_stock_history(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_type=adjust_type,
                    output_dir=data_source
                )
            elif market == 'hk':
                if not stock_code.startswith('HK') :
                    error_response_data = {'success': False, 'message': f'{market}股票代码请保证前缀HK: {stock_code}', 'data':{}}
                    error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                    error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return error_response
                success, filename = manager_futu.get_single_hk_stock_history(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_type=adjust_type,
                    output_dir=data_source
                )
            else:
                error_response_data = {'success': False, 'message': f'暂不支持的市场: {market}', 'data':{}}
                error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
                error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
                return error_response
        else:
            error_response_data = {'success': False, 'message': f'数据源 {data_source} 的数据获取功能尚未实现', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        if success:
            response_data = {
                'success': True,
                'message': f'股票数据获取成功！股票代码: {stock_code}, 数据文件已保存至: {filename}',
                'data':{
                    'filename': filename
                }
            }
            response = make_response(json.dumps(response_data, ensure_ascii=False))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return response

        else:
            error_response_data = {'success': False, 'message': '股票数据获取失败，请检查股票代码是否正确或稍后重试', 'data':{}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

    except Exception as e:
        logger.error(f"获取股票数据时出错: {str(e)}")
        error_response_data = {'success': False, 'message':f'获取数据时发生错误: {str(e)},请检查股票代码是否正确或稍后重试', 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/stocks/a_stock_list', methods=['GET'])
@log_request_details
def get_a_stock_list():
    """获取过滤后的A股股票列表"""
    try:
        exclude_indices = request.args.get('exclude_indices', '1') == '1'
        exclude_delisted = request.args.get('exclude_delisted', '1') == '1'
        exclude_st = request.args.get('exclude_st', '1') == '1'
        exclude_3xx = request.args.get('exclude_3xx', '1') == '1'
        include_prefix = request.args.get('include_prefix', '')

        exclude_prefixes = ['3'] if exclude_3xx else []
        include_prefixes = [p.strip() for p in include_prefix.split(',') if p.strip()] if include_prefix else None

        configs = get_filtered_stock_configs(
            exclude_indices=exclude_indices,
            exclude_delisted=exclude_delisted,
            exclude_st=exclude_st,
            exclude_prefixes=exclude_prefixes,
            include_prefixes=include_prefixes,
        )
        response_data = {
            'success': True,
            'message': f'获取到 {len(configs)} 只股票',
            'data': {'stocks': configs, 'count': len(configs)}
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取A股列表失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/stocks/check_integrity', methods=['POST'])
@log_request_details
def check_integrity():
    """扫描已有CSV，检查数据完整性，写入config"""
    try:
        data = request.json or {}
        data_source = data.get('data_source', 'baostock')
        target_start = data.get('target_start')
        target_end = data.get('target_end')
        stock_configs = data.get('stock_configs', [])

        config = do_check_integrity(data_source, target_start, target_end)

        plan_summary = None
        if stock_configs:
            plan = get_incomplete_stocks_for_download(config, stock_configs, data_source)
            plan_summary = {
                'full': len(plan['full']),
                'incremental': len(plan['incremental']),
                'skipped': plan['skipped'],
            }

        response_data = {
            'success': True,
            'message': f"完整性检查完成: 完整={config['summary']['complete']}, 不完整={config['summary']['incomplete']}",
            'data': config,
            'plan': plan_summary,
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"完整性检查失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/stocks/load_config', methods=['GET'])
@log_request_details
def load_config_endpoint():
    """读取 data_integrity config"""
    try:
        config = load_integrity_config()
        response_data = {'success': True, 'message': 'OK', 'data': config}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"读取config失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/cancel_download/<task_id>', methods=['POST'])
@log_request_details
def cancel_download_endpoint(task_id):
    """取消正在进行的下载任务"""
    try:
        result = cancel_download(task_id)
        if result:
            response_data = {'success': True, 'message': '已发送取消信号，正在同步config...'}
        else:
            response_data = {'success': False, 'message': '任务不存在'}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"取消下载失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e)}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/acquire_stock_data_batch', methods=['POST'])
@log_request_details
def acquire_stock_data_batch():
    """批量获取股票历史数据（异步启动，通过 task_id 轮询进度）"""
    try:
        data = request.json
        stock_configs = data.get('stock_configs', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        parallel = data.get('parallel', False)
        resume_task_id = data.get('resume_task_id', None)

        if not stock_configs:
            error_response_data = {'success': False, 'message': 'stock_configs不能为空', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        if resume_task_id:
            existing = get_download_progress(resume_task_id)
            if existing.get('status') == 'not_found':
                resume_task_id = None

        logger.info(f"启动批量下载 {len(stock_configs)} 只股票 (resume={resume_task_id})")

        task_id = start_batch_download(
            stock_configs=stock_configs,
            start_date=start_date,
            end_date=end_date,
            parallel=parallel,
            resume_task_id=resume_task_id,
        )
        response_data = {
            'success': True,
            'message': f'批量下载任务已启动，共 {len(stock_configs)} 只股票',
            'data': {'task_id': task_id, 'total': len(stock_configs)}
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"批量下载失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/download_progress/<task_id>', methods=['GET'])
@log_request_details
def get_download_progress_endpoint(task_id):
    """查询批量下载进度"""
    try:
        progress = get_download_progress(task_id)
        response = make_response(json.dumps({'success': True, 'message': 'OK', 'data': progress}, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        logger.error(f"获取下载进度失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/results/summary/<path:result_path>', methods=['GET'])
@log_request_details
def get_result_summary(result_path):
    """获取批量回测的汇总JSON"""
    try:
        summary_file = os.path.join(result_root, result_path)
        if not os.path.exists(summary_file):
            error_response_data = {'success': False, 'message': 'Summary not found', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response
        with open(summary_file, 'r', encoding='utf-8') as f:
            summary_data = json.load(f)
        response_data = {'success': True, 'message': 'OK', 'data': summary_data}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"读取汇总失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/signal_analysis')
@log_request_details
def signal_analysis():
    """信号分析页面"""
    return render_template('signal_analysis.html')


@app.route('/get_signal_files')
@log_request_details
def get_signal_files():
    """获取所有信号文件信息"""

    try:
        signal_files = signal_get()
        signal_files.sort(key=lambda x: x['file_time'], reverse=True)
        response_data = {
            'success': True,
            'message': f'Found {len(signal_files)} signal files',
            'data': {
                'signal_files': signal_files
            }
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取信号文件失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/analyze_signals', methods=['POST'])
@log_request_details
def analyze_signals():
    """分析信号文件"""

    try:
        data = request.json
        file_paths = data.get('file_paths', [])
        filters = data.get('filters', {})
        combined_df = signals_analyze(file_paths, filters)
        
        # 计算时间范围
        date_range = ''
        if len(combined_df) > 0:
            min_date = combined_df['date'].min()
            max_date = combined_df['date'].max()
            date_range = f"{min_date} 至 {max_date}"
        
        # 统计信号类型分布
        signal_type_counts = combined_df['signal_type'].value_counts().to_dict()
        
        response_data = {
            'success': True,
            'message': f'Found signals success',
            'data': {
                'signals': combined_df.to_dict('records'),
                'summary': {
                    'total_signals': len(combined_df),
                    'buy_signals': len(combined_df[combined_df['signal_type'].str.contains('buy')]),
                    'sell_signals': len(combined_df[combined_df['signal_type'].str.contains('sell')]),
                    'neutral_signals': len(combined_df[combined_df['signal_type'].str.contains('neutral')]),
                    'unique_stocks': combined_df['stock_info'].nunique(),
                    'unique_strategies': combined_df['strategy_name'].nunique(),
                    'date_range': date_range,
                    'signal_type_counts': signal_type_counts
                }
            }
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response

    except Exception as e:
        logger.error(f"分析信号失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response

@app.route('/get_signal_metadata')
@log_request_details
def get_signal_metadata():
    """获取信号元数据（用于筛选）"""
    try:
        if not os.path.exists(signals_root):
            response_data = {'success': False, 'message': '信号目录不存在', 'data':{}}
            response = make_response(json.dumps(response_data, ensure_ascii=False))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return response

        strategies = set()
        stock_codes = set()
        signal_types = set()

        # 遍历信号目录
        for root, dirs, files in os.walk(signals_root):
            for file in files:
                if file.endswith('.csv') and file.startswith('stock_signals_'):
                    file_path = os.path.join(root, file)

                    # 从路径中提取策略名称
                    relative_path = os.path.relpath(file_path, signals_root)
                    parts = relative_path.split(os.sep)
                    if len(parts) > 2:
                        strategies.add(parts[2])

                    # 从文件名中提取股票代码
                    if len(parts) > 1:
                        stock_codes.add(parts[1])

                    # 读取文件获取信号类型
                    try:
                        df = read_data(file_path)
                        if 'signal_type' in df.columns:
                            signal_types.update(df['signal_type'].unique())
                    except:
                        pass
        response_data = {
            'success': True,
            'message': 'get signal metadata success',
            'data':{
                'metadata': {
                    'strategies': list(strategies),
                    'stock_codes': list(stock_codes),
                    'signal_types': list(signal_types)
                }
            }

        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取信号元数据失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response
# 在现有API端点后添加新的端点
@app.route('/generate_html_report', methods=['POST'])
@log_request_details
def generate_html_report():
    """生成HTML报告"""
    try:
        data = request.get_json()
        signals_data = data.get('signals_data', [])
        filters = data.get('filters', {})
        summary = data.get('summary', {})

        if not signals_data:
            response_data = {'success': False, 'message': '没有可生成报告的信号数据', 'data':{}}
            response = make_response(json.dumps(response_data, ensure_ascii=False))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return response

        # 生成HTML报告
        html_content = signals_to_html(signals_data, filters, summary)

        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"信号分析报告_{timestamp}.html"

        logger.info(f"HTML信号分析报告已生成: {file_name}")
        logger.debug(f"HTML信号分析报告内容已生成: {html_content}")

        # 返回HTML内容和文件名，方便前端下载
        response_data = {
            'success': True,
            'message': 'HTML报告生成成功',
            'data':{
                'html_content': html_content,
                'file_name': file_name,
            }
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response

    except Exception as e:
        logger.error(f"生成HTML报告失败: {str(e)}")
        error_response_data = {'success': False, 'message': str(e), 'data':{}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response

@app.route('/strategy_code')
@log_request_details
def strategy_code():
    """策略代码查看页面"""
    strategies = global_strategy_manager.get_strategy_names()
    indicators = global_indicator_manager.get_indicator_names()
    return render_template('strategy_code.html', strategies=strategies, indicators=indicators)


@app.route('/get_strategy_code/<strategy_name>')
@log_request_details
def get_strategy_code(strategy_name):
    """获取策略代码"""
    try:
        code_info = global_strategy_manager.get_strategy_source_code(strategy_name)
        if not code_info:
            error_response_data = {'success': False, 'message': f'Strategy not found: {strategy_name}', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': 'Strategy code retrieved successfully',
            'data': code_info
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"Error getting strategy code: {str(e)}")
        error_response_data = {'success': False, 'message': f'Error getting strategy code: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/get_indicator_code/<indicator_name>')
@log_request_details
def get_indicator_code(indicator_name):
    """获取指标类的源代码"""
    try:
        indicator_info = global_indicator_manager.get_indicator_source_code(indicator_name)
        if not indicator_info:
            error_response_data = {'success': False, 'message': f'Indicator not found: {indicator_name}', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': 'Indicator code retrieved successfully',
            'data': indicator_info
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"Error getting indicator code: {str(e)}")
        error_response_data = {'success': False, 'message': f'Error getting indicator code: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/schedule')
@log_request_details
def schedule_page():
    """定时任务管理页面"""
    strategies = global_strategy_manager.get_strategy_names()
    return render_template('schedule.html', strategies=strategies)


@app.route('/api/tasks/get_all', methods=['GET'])
@log_request_details
def get_all_tasks():
    """获取所有任务"""
    try:
        tasks = task_manager.read_all()
        response_data = {
            'success': True,
            'message': f'获取到 {len(tasks)} 个任务',
            'data': tasks
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取任务列表失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取任务列表失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/get/<task_id>', methods=['GET'])
@log_request_details
def get_task_by_id(task_id):
    """根据ID获取任务"""
    try:
        task = task_manager.read(task_id)
        if not task:
            error_response_data = {'success': False, 'message': f'任务不存在: {task_id}', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '获取任务成功',
            'data': task
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/create', methods=['POST'])
@log_request_details
def create_task():
    """创建新任务"""
    try:
        task_data = request.json
        if not task_data:
            error_response_data = {'success': False, 'message': '任务数据不能为空', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        created_task = task_manager.create(task_data)
        if not created_task:
            error_response_data = {'success': False, 'message': '创建任务失败', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '创建任务成功',
            'data': created_task
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"创建任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'创建任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/update/<task_id>', methods=['POST'])
@log_request_details
def update_task_by_id(task_id):
    """更新任务"""
    try:
        request_data = request.json
        if not request_data:
            error_response_data = {'success': False, 'message': '更新数据不能为空', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response
        update_data = request_data.get('taskData', request_data)
        updated_task = task_manager.update(task_id, update_data)
        if not updated_task:
            error_response_data = {'success': False, 'message': f'更新任务失败: 任务不存在', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '更新任务成功',
            'data': updated_task
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"更新任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'更新任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/delete/<task_id>', methods=['POST'])
@log_request_details
def delete_task_by_id(task_id):
    """删除任务"""
    try:
        success = task_manager.delete(task_id)
        if not success:
            error_response_data = {'success': False, 'message': f'删除任务失败: 任务不存在', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '删除任务成功',
            'data': {}
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"删除任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'删除任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/query', methods=['POST'])
@log_request_details
def query_tasks():
    """根据条件查询任务"""
    try:
        filters = request.json
        tasks = task_manager.query(filters)

        response_data = {
            'success': True,
            'message': f'查询到 {len(tasks)} 个符合条件的任务',
            'data': tasks
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"查询任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'查询任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/enable/<task_id>', methods=['POST'])
@log_request_details
def enable_task(task_id):
    """启用任务"""
    try:
        enabled_task = task_manager.enable(task_id)
        if not enabled_task:
            error_response_data = {'success': False, 'message': f'启用任务失败: 任务不存在', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '启用任务成功',
            'data': enabled_task
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"启用任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'启用任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/disable/<task_id>', methods=['POST'])
@log_request_details
def disable_task(task_id):
    """禁用任务"""
    try:
        disabled_task = task_manager.disable(task_id)
        if not disabled_task:
            error_response_data = {'success': False, 'message': f'禁用任务失败: 任务不存在', 'data': {}}
            error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
            error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return error_response

        response_data = {
            'success': True,
            'message': '禁用任务成功',
            'data': disabled_task
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"禁用任务失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'禁用任务失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/count', methods=['GET'])
@log_request_details
def get_task_count():
    """获取任务总数"""
    try:
        count = task_manager.count()
        response_data = {
            'success': True,
            'message': '获取任务数量成功',
            'data': {'count': count}
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取任务数量失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取任务数量失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/tasks/exists/<task_id>', methods=['GET'])
@log_request_details
def check_task_exists(task_id):
    """检查任务是否存在"""
    try:
        exists = task_manager.exists(task_id)
        response_data = {
            'success': True,
            'message': '检查任务存在状态成功',
            'data': {'exists': exists}
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"检查任务存在状态失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'检查任务存在状态失败: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/executions/get_all', methods=['GET'])
@log_request_details
def get_all_executions():
    """获取所有执行记录"""
    try:
        limit = request.args.get('limit', 100, type=int)
        executions = task_execution_manager.read_all(limit=limit)
        response_data = {'success': True, 'message': '获取执行记录成功', 'data': executions}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取执行记录失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取执行记录失败: {str(e)}', 'data': []}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/executions/get/<execution_id>', methods=['GET'])
@log_request_details
def get_execution(execution_id):
    """获取单个执行记录"""
    try:
        execution = task_execution_manager.read(execution_id)
        if execution:
            response_data = {'success': True, 'message': '获取执行记录成功', 'data': execution}
        else:
            response_data = {'success': False, 'message': '执行记录不存在', 'data': None}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取执行记录失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取执行记录失败: {str(e)}', 'data': None}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/executions/by_task/<task_id>', methods=['GET'])
@log_request_details
def get_executions_by_task(task_id):
    """获取指定任务的执行记录"""
    try:
        limit = request.args.get('limit', 50, type=int)
        executions = task_execution_manager.read_by_task(task_id, limit=limit)
        response_data = {'success': True, 'message': '获取执行记录成功', 'data': executions}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"获取执行记录失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'获取执行记录失败: {str(e)}', 'data': []}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/api/executions/delete/<execution_id>', methods=['POST'])
@log_request_details
def delete_execution(execution_id):
    """删除执行记录"""
    try:
        success = task_execution_manager.delete(execution_id)
        if success:
            response_data = {'success': True, 'message': '删除执行记录成功'}
        else:
            response_data = {'success': False, 'message': '删除执行记录失败'}
        response = make_response(json.dumps(response_data, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    except Exception as e:
        logger.error(f"删除执行记录失败: {str(e)}")
        error_response_data = {'success': False, 'message': f'删除执行记录失败: {str(e)}'}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response


@app.route('/chat')
@log_request_details
def chat_page():
    """AI聊天页面"""
    return render_template('chat.html')

@app.route('/music_player')
@log_request_details
def music_player_page():
    """背景音乐播放器页面"""
    return render_template('music_player.html')

@app.route('/api/ai-chat', methods=['POST'])
@log_request_details
def chat():
    data = request.json
    model = data.get('type')
    prompt = data.get('prompt', '')
    if not prompt:
        error_response_data = {'success': False, 'message': f'请输入内容: {str(e)}', 'data': {}}
        error_response = make_response(json.dumps(error_response_data, ensure_ascii=False))
        error_response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return error_response
    logger.info(f'ai model is {model}')
    ai_manager = AIManager(model)
    result = ai_manager.get_response(prompt)
    logger.info(f'ai prompt is {prompt}')
    logger.info(f'ai result is {result}')

    response_data = {
        'success': True,
        'message': f'Success',
        'data': {'response': result}
    }
    response = make_response(json.dumps(response_data, ensure_ascii=False))
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    return response


if __name__ == '__main__':
    # 在开发环境中运行，生产环境应使用WSGI服务器
    try:
        # 在Flask debug模式下，避免在子进程中重复启动任务调度器
        import os
        # 检查是否为主进程（Flask会设置WERKZEUG_RUN_MAIN环境变量标识子进程）
        if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
            # 创建进程启动schedule_tasks
            task_process = multiprocessing.Process(target=schedule_tasks)
            task_process.daemon = True  # 设置为守护进程，主进程结束时自动终止
            task_process.start()
            logger.info("启动任务定时器")
        else:
            logger.info("在Flask子进程中，不启动任务定时器")
    except KeyboardInterrupt:
        logger.info("用户中断，停止任务定时器")
    except Exception as e:
        logger.error(f"任务定时器异常: {str(e)}")
    app.run(debug=True, host='0.0.0.0', port=5000)
