from dataclasses import dataclass, field, asdict
from typing import List

from common.logger import create_log

logger = create_log('backtest_result')


@dataclass
class BacktestResult:
    stock_code: str = ""
    stock_name: str = ""
    market: str = ""
    csv_path: str = ""
    success: bool = False
    low_data: bool = False
    error: str = ""

    total_return_pct: float = 0.0
    annual_return_pct: float = 0.0
    final_cash: float = 0.0

    max_drawdown_pct: float = 0.0
    calmar_ratio: float = 0.0
    sharpe_ratio: float = 0.0

    total_trades: int = 0
    won_trades: int = 0
    lost_trades: int = 0
    win_rate_pct: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0

    buy_signals: int = 0
    sell_signals: int = 0
    executed_buys: int = 0
    executed_sells: int = 0

    start_date: str = ""
    end_date: str = ""
    data_days: int = 0

    avg_holding_days: float = 0.0

    html_path: str = ""
    signal_path: str = ""


def aggregate_backtest_results(results: List[BacktestResult]) -> dict:
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    traded = [r for r in successful if r.total_trades > 0]

    stock_count = len(results)
    success_count = len(successful)
    failed_count = len(failed)
    low_data_count = sum(1 for r in successful if r.low_data)

    total_trades_all = sum(r.total_trades for r in successful)
    won_trades_all = sum(r.won_trades for r in successful)
    lost_trades_all = sum(r.lost_trades for r in successful)

    weighted_win_rate_pct = (won_trades_all / total_trades_all * 100) if total_trades_all > 0 else 0.0

    if traded:
        weighted_profit_factor = (
            (sum(r.won_trades * r.avg_win for r in traded) / sum(r.won_trades for r in traded)) /
            (sum(r.lost_trades * r.avg_loss for r in traded) / sum(r.lost_trades for r in traded))
        ) if sum(r.won_trades for r in traded) > 0 and sum(r.lost_trades for r in traded) > 0 else 0.0
    else:
        weighted_profit_factor = 0.0

    total_data_years = sum(r.data_days for r in successful) / 365.0
    avg_trades_per_year = total_trades_all / total_data_years if total_data_years > 0 else 0.0

    holding_days_all = []
    for r in successful:
        if r.avg_holding_days > 0:
            holding_days_all.append(r.avg_holding_days)
    avg_holding_days_overall = sum(holding_days_all) / len(holding_days_all) if holding_days_all else 0.0

    summary = {
        'stock_count': stock_count,
        'success_count': success_count,
        'failed_count': failed_count,
        'low_data_count': low_data_count,
        'total_trades_all': total_trades_all,
        'won_trades_all': won_trades_all,
        'lost_trades_all': lost_trades_all,
        'weighted_win_rate_pct': round(weighted_win_rate_pct, 2),
        'weighted_profit_factor': round(weighted_profit_factor, 2),
        'total_data_years': round(total_data_years, 2),
        'avg_trades_per_year': round(avg_trades_per_year, 2),
        'avg_holding_days': round(avg_holding_days_overall, 2),
    }

    if successful:
        summary['equal_weight'] = {
            'avg_annual_return_pct': round(sum(r.annual_return_pct for r in successful) / len(successful), 2),
            'avg_max_drawdown_pct': round(sum(r.max_drawdown_pct for r in successful) / len(successful), 2),
            'avg_sharpe_ratio': round(sum(r.sharpe_ratio for r in successful) / len(successful), 2),
            'avg_win_rate_pct': round(sum(r.win_rate_pct for r in successful) / len(successful), 2),
            'avg_profit_factor': round(sum(r.profit_factor for r in successful) / len(successful), 2),
        }
    else:
        summary['equal_weight'] = {}

    logger.info("=" * 60)
    logger.info("【批量回测汇总统计】")
    logger.info(f"股票总数: {stock_count}, 成功: {success_count}, 失败: {failed_count}")
    logger.info(f"加权胜率: {weighted_win_rate_pct:.2f}%")
    logger.info(f"加权盈亏比: {weighted_profit_factor:.2f}")
    logger.info(f"总交易次数: {total_trades_all}")
    logger.info(f"年均交易次数: {avg_trades_per_year:.2f}")
    logger.info(f"平均持仓天数: {avg_holding_days_overall:.2f}")
    logger.info("=" * 60)

    return summary


def results_to_csv_data(results: List[BacktestResult]) -> list[dict]:
    rows = []
    for r in results:
        rows.append({
            '股票代码': r.stock_code,
            '股票名称': r.stock_name,
            '市场': r.market,
            '成功': r.success,
            '数据天数': r.data_days,
            '开始日期': r.start_date,
            '结束日期': r.end_date,
            '总收益率%': round(r.total_return_pct, 2),
            '年化收益%': round(r.annual_return_pct, 2),
            '最大回撤%': round(r.max_drawdown_pct, 2),
            '夏普比率': round(r.sharpe_ratio, 2),
            '总交易': r.total_trades,
            '盈利交易': r.won_trades,
            '亏损交易': r.lost_trades,
            '胜率%': round(r.win_rate_pct, 2),
            '盈亏比': round(r.profit_factor, 2),
            '平均盈利': round(r.avg_win, 2),
            '平均亏损': round(r.avg_loss, 2),
            '买入信号': r.buy_signals,
            '卖出信号': r.sell_signals,
            '平均持仓天': round(r.avg_holding_days, 2),
            '错误': r.error if r.error else '',
        })
    return rows
