import datetime as dt
import json
import os

import backtrader as bt
import pandas as pd

from common.logger import create_log
from common.time_key import get_current_time
from core.quant.backtest_result import BacktestResult, aggregate_backtest_results, results_to_csv_data
from core.strategy.trading.trading_commition import CommissionFactory
from core.visualization.visual_tools_plotly import plotly_draw
from pathlib import Path
import settings

logger = create_log('quant_manage')


def _calc_avg_holding_days(trade_record_manager):
    records = trade_record_manager.trade_records
    if not records:
        return 0.0
    buys = [r for r in records if r.action == 'B']
    sells = [r for r in records if r.action == 'S']
    buys.sort(key=lambda r: r.date)
    sells.sort(key=lambda r: r.date)

    if not buys or not sells:
        return 0.0

    buy_idx = 0
    holding_days_list = []
    # copy sizes to avoid mutating originals
    buy_remaining = [b.size for b in buys]

    for sell in sells:
        remaining = sell.size
        while remaining > 0 and buy_idx < len(buys):
            matched = min(buy_remaining[buy_idx], remaining)
            if matched > 0:
                hd = (sell.date - buys[buy_idx].date).days
                if hd >= 0:
                    holding_days_list.append(hd)
            buy_remaining[buy_idx] -= matched
            remaining -= matched
            if buy_remaining[buy_idx] <= 0:
                buy_idx += 1

    return sum(holding_days_list) / len(holding_days_list) if holding_days_list else 0.0


def _extract_stock_info(df: pd.DataFrame) -> dict:
    info = {'stock_code': '', 'stock_name': '', 'market': 'HK'}
    try:
        if 'stock_code' in df.columns:
            info['stock_code'] = str(df['stock_code'].iloc[0])
        if 'stock_name' in df.columns:
            info['stock_name'] = str(df['stock_name'].iloc[0])
        if 'market' in df.columns:
            info['market'] = str(df['market'].iloc[0])
    except Exception:
        pass
    return info


def get_data_form_csv(csv_path, fromdate=None, todate=None):
    df = pd.read_csv(
        csv_path,
        parse_dates=['date'],
        index_col='date'
    )

    # filter by date range if specified
    if fromdate is not None or todate is not None:
        if fromdate is not None:
            fromdate = pd.Timestamp(fromdate) if not isinstance(fromdate, pd.Timestamp) else fromdate
            df = df[df.index >= fromdate]
        if todate is not None:
            todate = pd.Timestamp(todate) if not isinstance(todate, pd.Timestamp) else todate
            df = df[df.index <= todate]
        if df.empty:
            raise ValueError(f"日期范围 {fromdate} ~ {todate} 内无数据")

    class CustomPandasData(bt.feeds.PandasData):
        params = (
            ('datetime', None),
            ('open', 'open'), ('high', 'high'), ('low', 'low'), ('close', 'close'), ('volume', 'volume'),
            ('market', 'market'),
            ('openinterest', -1)
        )

    data_feed = CustomPandasData(dataname=df)
    data_feed.timeframe = bt.TimeFrame.Days
    data_feed.compression = 1

    return data_feed


def run_backtest_enhanced_volume_strategy_multi(
    kline_csv_folder_path,
    trading_strategy: bt.Strategy,
    init_cash=settings.INIT_CASH,
    cut_date_range=None
):
    """
    批量运行增强成交量策略回测，汇总统计
    :param kline_csv_folder_path: 包含CSV文件的文件夹路径
    :param trading_strategy: 交易策略类
    :param init_cash: 初始资金
    :param cut_date_range: 可选 (fromdate, todate) 统一起止日期
    :return: (results_list, summary_dict)
    """
    batch_timestamp = get_current_time()
    strategy_name = trading_strategy.__name__
    fromdate, todate = None, None
    if cut_date_range:
        fromdate, todate = cut_date_range

    folder = Path(kline_csv_folder_path)
    csv_files = sorted(folder.glob("*.csv"))
    total_files = len(csv_files)

    logger.info("=" * 60)
    logger.info(f"【批量回测启动】文件夹: {kline_csv_folder_path}")
    logger.info(f"【文件数量】{total_files} 个CSV文件")
    logger.info(f"【策略】{strategy_name} | 初始资金: {init_cash:,.2f}")
    if cut_date_range:
        logger.info(f"【日期范围】{fromdate} ~ {todate}")
    logger.info("=" * 60)

    all_results = []
    batch_result_dir = settings.result_root / strategy_name / batch_timestamp

    for idx, csv_path in enumerate(csv_files):
        logger.info(f"\n>>> [{idx + 1}/{total_files}] 回测: {csv_path.name}")
        result = run_backtest_enhanced_volume_strategy(
            csv_path, trading_strategy, init_cash,
            batch_timestamp=batch_timestamp,
            fromdate=fromdate, todate=todate,
            batch_result_dir=batch_result_dir,
        )
        all_results.append(result)

    summary = aggregate_backtest_results(all_results)

    # save batch summary CSV
    try:
        os.makedirs(batch_result_dir, exist_ok=True)
        csv_rows = results_to_csv_data(all_results)
        batch_csv_path = batch_result_dir / "batch_summary.csv"
        pd.DataFrame(csv_rows).to_csv(batch_csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"批量汇总表已保存: {batch_csv_path}")

        summary_path = batch_result_dir / "summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info(f"汇总JSON已保存: {summary_path}")
    except Exception as e:
        logger.warning(f"保存汇总文件失败: {e}")

    logger.info("=" * 60)
    logger.info("【批量回测全部完成】")
    logger.info("=" * 60)

    return all_results, summary, str(batch_result_dir)


def run_backtest_enhanced_volume_strategy(
    csv_path,
    trading_strategy: bt.Strategy,
    init_cash=settings.INIT_CASH,
    batch_timestamp=None,
    fromdate=None,
    todate=None,
    batch_result_dir=None,
) -> BacktestResult:
    current_time = batch_timestamp if batch_timestamp else get_current_time()
    relative_path = str(csv_path).replace(str(settings.stock_data_root) + os.sep, '')
    csv_path_obj = Path(csv_path)

    result = BacktestResult(csv_path=str(csv_path))

    logger.info("=" * 60)
    logger.info("【程序启动】策略回测程序")
    logger.info(f"【目标文件】{csv_path}")
    logger.info("=" * 60)

    logger.info("【回测配置】开始初始化回测参数")

    # 加载数据
    raw_df = None
    try:
        raw_df = pd.read_csv(csv_path, parse_dates=['date'])
        try:
            data = get_data_form_csv(csv_path, fromdate=fromdate, todate=todate)
        except ValueError as ve:
            logger.warning(f"【回测终止】{str(ve)}")
            result.error = str(ve)
            return result
    except Exception as e:
        logger.warning(f"【回测终止】数据加载失败：{str(e)}")
        result.error = str(e)
        return result

    data_length = len(data.p.dataname)
    logger.info(f"【数据检查】有效数据量：{data_length} 天")

    stock_info = _extract_stock_info(data.p.dataname)
    result.stock_code = stock_info['stock_code']
    result.stock_name = stock_info['stock_name']
    result.market = stock_info['market']

    result.low_data = data_length < 50
    try:
        result.start_date = str(data.p.dataname.index[0].date())
        result.end_date = str(data.p.dataname.index[-1].date())
        result.data_days = data_length
    except Exception:
        pass

    if result.low_data:
        logger.info("【风险提示】数据量较少，可能影响策略信号有效性！")

    market = stock_info.get('market', 'HK')
    if not market:
        market = 'HK'

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.broker.set_cash(init_cash)
    commission = CommissionFactory.get_commission(market)
    cerebro.broker.addcommissioninfo(commission)
    cerebro.broker.set_slippage_fixed(commission.p.slippage)
    cerebro.broker.set_coc(True)
    logger.info(
        f"【资金配置】初始资金：{init_cash:,.2f} | 佣金率：{commission.p.commission:.2f}% | 滑点：{commission.p.slippage:.2f}")
    logger.info("=" * 60)

    cerebro.addstrategy(trading_strategy)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="total_return", timeframe=bt.TimeFrame.NoTimeFrame)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trade_analyzer")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe_ratio", timeframe=bt.TimeFrame.Days, riskfreerate=0.03)
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_return")

    logger.info(f"【回测启动】初始资金：{cerebro.broker.getcash():,.2f}")
    logger.info(f"【回测周期】：{result.start_date} ~ {result.end_date}")
    logger.info("=" * 60)

    logger.info("【回测执行】正在运行回测...")
    try:
        bt_results = cerebro.run()
    except Exception as e:
        logger.warning(f"【回测失败】执行出错：{str(e)}")
        result.error = str(e)
        return result
    strategy = bt_results[0]

    logger.info("【回测结果汇总】")
    logger.info("=" * 60)

    # 收益
    total_return = 0.0
    annual_return = 0.0
    days = result.data_days
    try:
        total_return = list(strategy.analyzers.total_return.get_analysis().values())[0] * 100
        result.final_cash = cerebro.broker.getvalue()
        annual_return = (pow((1 + total_return / 100), 365 / days) - 1) * 100 if days > 0 else 0
        result.total_return_pct = total_return
        result.annual_return_pct = annual_return
        logger.info(f"1. 收益情况：总收益率={total_return:.2f}% | 年化收益={annual_return:.2f}% | 最终资金={result.final_cash:,.2f}")
    except Exception as e:
        logger.warning(f"1. 收益情况：无法计算 ({str(e)})")

    # 风险
    max_dd = 0.0
    try:
        max_dd = strategy.analyzers.drawdown.get_analysis()["max"]["drawdown"]
        result.max_drawdown_pct = max_dd
        try:
            calmar_ratio = annual_return / max_dd if max_dd > 0 else 0
            result.calmar_ratio = calmar_ratio
        except Exception:
            calmar_ratio = 0
        logger.info(f"2. 风险指标：最大回撤={max_dd:.2f}% | Calmar比率={calmar_ratio:.2f}")
    except Exception as e:
        logger.warning(f"2. 风险指标：无法计算 ({str(e)})")

    # 交易
    total_trades = 0
    won_trades = 0
    lost_trades = 0
    win_rate = 0.0
    avg_win = 0.0
    avg_loss = 0.0
    profit_factor = 0.0
    try:
        trade_stats = strategy.analyzers.trade_analyzer.get_analysis()
        total_trades = trade_stats["total"]["total"]
        won_trades = trade_stats.get("won", {}).get("total", 0)
        lost_trades = trade_stats.get("lost", {}).get("total", 0)
        win_rate = (won_trades / total_trades) * 100 if total_trades > 0 else 0
        try:
            avg_win = trade_stats.get("won", {}).get("pnl", {}).get("average", 0)
            avg_loss = abs(trade_stats.get("lost", {}).get("pnl", {}).get("average", 1))
            profit_factor = avg_win / avg_loss if avg_loss > 0 else 0
        except Exception:
            profit_factor = 0
        result.total_trades = total_trades
        result.won_trades = won_trades
        result.lost_trades = lost_trades
        result.win_rate_pct = win_rate
        result.avg_win = avg_win
        result.avg_loss = avg_loss
        result.profit_factor = profit_factor
        logger.info(f"3. 交易统计：总交易={total_trades} | 盈利={won_trades} | 亏损={lost_trades} | 胜率={win_rate:.2f}% | 盈亏比={profit_factor:.2f}")
    except Exception as e:
        logger.warning(f"3. 交易统计：无法计算 ({str(e)})")

    # 夏普
    try:
        sharpe_ratio = strategy.analyzers.sharpe_ratio.get_analysis().get("sharperatio", 0)
        sharpe_ratio = sharpe_ratio if sharpe_ratio is not None else 0
        result.sharpe_ratio = sharpe_ratio
        logger.info(f"4. 风险调整收益：夏普比率={sharpe_ratio:.2f}")
    except Exception as e:
        logger.warning(f"4. 风险调整收益：无法计算 ({str(e)})")

    # 信号
    try:
        result.buy_signals = strategy.buy_signals_count
        result.sell_signals = strategy.sell_signals_count
        result.executed_buys = strategy.executed_buys_count
        result.executed_sells = strategy.executed_sells_count
        logger.info(f"5. 信号统计：买入信号={result.buy_signals} | 卖出信号={result.sell_signals} | 实际买入={result.executed_buys} | 实际卖出={result.executed_sells}")
    except Exception as e:
        logger.warning(f"5. 信号统计：无法计算 ({str(e)})")

    # 平均持仓天数
    try:
        if hasattr(strategy, 'trade_record_manager'):
            avg_hd = _calc_avg_holding_days(strategy.trade_record_manager)
            result.avg_holding_days = avg_hd
            logger.info(f"5.1 平均持仓天数：{avg_hd:.1f} 天")
    except Exception as e:
        logger.warning(f"5.1 平均持仓天数：无法计算 ({str(e)})")

    # 保存信号记录 (保持 signals/ 目录不变)
    try:
        if hasattr(strategy, 'indicator') and hasattr(strategy.indicator, 'signal_record_manager'):
            signals_df = strategy.indicator.signal_record_manager.transform_to_dataframe()
            if not signals_df.empty:
                signal_file_folder = settings.signals_root / relative_path.rsplit('.', 1)[0] / strategy.__class__.__name__
                os.makedirs(signal_file_folder, exist_ok=True)
                signals_file_path = os.path.join(signal_file_folder, f"stock_signals_{current_time}.csv")
                signals_df.to_csv(signals_file_path, index=False, encoding='utf-8-sig')
                result.signal_path = str(signals_file_path)
                logger.info(f"6. 信号记录已保存至：{signals_file_path}")
    except Exception as e:
        logger.warning(f"信号保存失败：{str(e)}")

    # 构建结果目录结构: results/{strategy}/{timestamp}/{stock}/chart.html
    stock_dir_name = f"{result.stock_code}_{result.stock_name}"
    if batch_result_dir:
        result_dir_for_stock = batch_result_dir / stock_dir_name
    else:
        strategy_name = strategy.__class__.__name__
        result_dir_for_stock = settings.result_root / strategy_name / current_time / stock_dir_name

    os.makedirs(result_dir_for_stock, exist_ok=True)
    html_file_name = "chart.html"
    html_path = plotly_draw(csv_path, strategy, init_cash, html_file_name, result_dir_for_stock)
    result.html_path = str(html_path)
    logger.info(f"7. 回测可视化图表已保存至：{html_path}")

    # 保存 metrics.json
    metrics_path = result_dir_for_stock / "metrics.json"
    try:
        metrics_dict = {
            'stock_code': result.stock_code,
            'stock_name': result.stock_name,
            'market': result.market,
            'csv_path': result.csv_path,
            'total_return_pct': round(result.total_return_pct, 2),
            'annual_return_pct': round(result.annual_return_pct, 2),
            'final_cash': round(result.final_cash, 2),
            'max_drawdown_pct': round(result.max_drawdown_pct, 2),
            'calmar_ratio': round(result.calmar_ratio, 2),
            'sharpe_ratio': round(result.sharpe_ratio, 2),
            'total_trades': result.total_trades,
            'won_trades': result.won_trades,
            'lost_trades': result.lost_trades,
            'win_rate_pct': round(result.win_rate_pct, 2),
            'avg_win': round(result.avg_win, 2),
            'avg_loss': round(result.avg_loss, 2),
            'profit_factor': round(result.profit_factor, 2),
            'buy_signals': result.buy_signals,
            'sell_signals': result.sell_signals,
            'executed_buys': result.executed_buys,
            'executed_sells': result.executed_sells,
            'start_date': result.start_date,
            'end_date': result.end_date,
            'data_days': result.data_days,
            'avg_holding_days': round(result.avg_holding_days, 1),
            'low_data': result.low_data,
            'error': result.error,
        }
        with open(metrics_path, 'w', encoding='utf-8') as f:
            json.dump(metrics_dict, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存metrics.json失败: {e}")

    result.success = True
    logger.info("=" * 60)
    logger.info("【回测结束】\n")

    return result


def get_file_names_pathlib(folder_path):
    folder = Path(folder_path)
    files = [f.name for f in folder.rglob('*') if f.is_file()]
    return files


if __name__ == "__main__":
    from settings import stock_data_root
    from core.strategy.trading.volume.enhanced_volume import EnhancedVolumeStrategy

    init_cash = 5000000
    csv_path = stock_data_root / "futu/HK.00700_腾讯控股_20220414_20260414.csv"
    # 单只
    run_backtest_enhanced_volume_strategy(csv_path, EnhancedVolumeStrategy, init_cash)
    # 批量
    run_backtest_enhanced_volume_strategy_multi(stock_data_root / "futu", EnhancedVolumeStrategy, init_cash)
