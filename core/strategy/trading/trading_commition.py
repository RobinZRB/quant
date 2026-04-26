import settings
from common.logger import create_log
import backtrader as bt

logger = create_log('commission')


class CommissionFactory:
    """佣金模型工厂"""
    @staticmethod
    def get_commission(market: str):
        """根据市场类型获取佣金模型"""
        if market == 'HK':
            return HKCommission()
        elif market == 'US':
            return USCommission()
        elif market == 'CN':
            return CNCommission()
        else:
            logger.warning(f"不支持的市场类型: {market}，使用港股佣金模型作为默认值")
            return HKCommission()   # default to HK


class HKCommission(bt.CommInfoBase):
    """港股市场佣金模型"""

    params = (
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('commission', settings.HK_COMMISSION if hasattr(settings, 'HK_COMMISSION') else 0.0003),  # 佣金率0.03%
        ('mincommission', settings.HK_MIN_COMMISSION if hasattr(settings, 'HK_MIN_COMMISSION') else 3),  # 最低佣金
        ('currency', settings.HK_CURRENCY if hasattr(settings, 'HK_CURRENCY') else 'HKD'),
        ('stamp_duty', settings.HK_STAMP_DUTY if hasattr(settings, 'HK_STAMP_DUTY') else 0.001),  # 印花税0.1%
        ('transaction_levy', settings.HK_TRANSACTION_LEVY if hasattr(settings, 'HK_TRANSACTION_LEVY') else 0.000042),  # 交易征费0.0042%
        ('transaction_fee', settings.HK_TRANSACTION_FEE if hasattr(settings, 'HK_TRANSACTION_FEE') else 0.0000565),  # 交易费0.00565%
        ('trading_system_fee', settings.HK_TRADING_SYSTEM_FEE if hasattr(settings, 'HK_TRADING_SYSTEM_FEE') else 15),  # 交易系统使用费15港币/笔
        ('settlement_fee', settings.HK_SETTLEMENT_FEE if hasattr(settings, 'HK_SETTLEMENT_FEE') else 0.00002),  # 股份交收费0.002%
        ('min_settlement_fee', settings.HK_MIN_SETTLEMENT_FEE if hasattr(settings, 'HK_MIN_SETTLEMENT_FEE') else 2),  # 最低交收费2港币
        ('max_settlement_fee', settings.HK_MAX_SETTLEMENT_FEE if hasattr(settings, 'HK_MAX_SETTLEMENT_FEE') else 100),  # 最高交收费100港币
        ('slippage', settings.HK_SLIPPAGE if hasattr(settings, 'HK_SLIPPAGE') else 0.3),    #滑点0.3港币
    )

    def _getcommission(self, size, price, pseudoexec):
        value = abs(size) * price
        # 佣金计算
        commission = max(value * self.p.commission, self.p.mincommission)
        stamp_duty = value * self.p.stamp_duty
        transaction_levy = value * self.p.transaction_levy
        transaction_fee = value * self.p.transaction_fee
        # 交收费（有上下限）
        settlement_fee = value * self.p.settlement_fee
        settlement_fee = max(min(settlement_fee, self.p.max_settlement_fee), self.p.min_settlement_fee)

        return commission + stamp_duty + transaction_levy + transaction_fee + self.p.trading_system_fee + settlement_fee

class CNCommission(bt.CommInfoBase):
    """A股市场佣金模型"""

    params = (
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('commission', settings.CN_COMMISSION if hasattr(settings, 'CN_COMMISSION') else 0.0003),  # 佣金率0.03%
        ('mincommission', settings.CN_MIN_COMMISSION if hasattr(settings, 'CN_MIN_COMMISSION') else 3),  # 最低佣金
        ('currency', settings.CN_CURRENCY if hasattr(settings, 'CN_CURRENCY') else 'CNY'),
        ('stamp_duty', settings.CN_STAMP_DUTY if hasattr(settings, 'CN_STAMP_DUTY') else 0.00025),  # 印花税0.05%，仅卖出收，所以买入+卖出相当于分别025%
        ('transaction_levy', settings.CN_TRANSACTION_LEVY if hasattr(settings, 'CN_TRANSACTION_LEVY') else 0.0000341),  # 经手费0.00341%
        ('transaction_fee', settings.CN_TRANSACTION_FEE if hasattr(settings, 'CN_TRANSACTION_FEE') else 0.00002),  # 政管费0.002%
        ('trading_system_fee', settings.CN_TRADING_SYSTEM_FEE if hasattr(settings, 'CN_TRADING_SYSTEM_FEE') else 15),  # 交易系统使用费15元/笔
        ('settlement_fee', settings.CN_SETTLEMENT_FEE if hasattr(settings, 'CN_SETTLEMENT_FEE') else 0.00002),  # 过户费0.002%
        ('slippage', settings.CN_SLIPPAGE if hasattr(settings, 'CN_SLIPPAGE') else 0.3),    #滑点0.3元
    )

    def _getcommission(self, size, price, pseudoexec):
        value = abs(size) * price
        # 佣金计算
        commission = max(value * self.p.commission, self.p.mincommission)
        stamp_duty = value * self.p.stamp_duty
        transaction_levy = value * self.p.transaction_levy
        transaction_fee = value * self.p.transaction_fee
        settlement_fee = value * self.p.settlement_fee

        return commission + stamp_duty + transaction_levy + transaction_fee + self.p.trading_system_fee + settlement_fee

class USCommission(bt.CommInfoBase):
    """美股市场佣金模型"""

    params = (
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('commission_per_share', settings.US_COMMISSION_PER_SHARE if hasattr(settings, 'US_COMMISSION_PER_SHARE') else 0.0049),  # 佣金率0.0049%/股
        ('min_commission', settings.US_MIN_COMMISSION if hasattr(settings, 'US_MIN_COMMISSION') else 0.99),  # 最低佣金0.99美元
        ('max_commission_rate', settings.US_MAX_COMMISSION_RATE if hasattr(settings, 'US_MAX_COMMISSION_RATE') else 0.005),  # 最高佣金率0.5%
        ('currency', settings.US_CURRENCY if hasattr(settings, 'US_CURRENCY') else 'USD'),
        ('min_trading_system_per_share', settings.US_MIN_TRADING_SYSTEM_PER_SHARE if hasattr(settings, 'US_MIN_TRADING_SYSTEM_PER_SHARE') else 0.005), # 交易系统使用费0.005美元/股
        ('max_trading_system_rate', settings.US_MAX_TRADING_SYSTEM_RATE if hasattr(settings, 'US_MAX_TRADING_SYSTEM_RATE') else 0.005),  # 交易系统使用费最高0.5%
        ('min_trading_system_fee', settings.US_MIN_TRADING_SYSTEM_FEE if hasattr(settings, 'US_MIN_TRADING_SYSTEM_FEE') else 1),  # 交易系统使用费最低1美元/笔
        ('settlement_fee', settings.US_SETTLEMENT_FEE if hasattr(settings, 'US_SETTLEMENT_FEE') else 0.00003),  # 股份交收费0.003%
        ('settlement_activity_fee_per_share', settings.US_SETTLEMENT_ACTIVITY_FEE_PER_SHARE if hasattr(settings, 'US_SETTLEMENT_ACTIVITY_FEE_PER_SHARE') else 0.000166),  # 交易活动费0.000166美元/股
        ('min_settlement_activity_fee', settings.US_MIN_SETTLEMENT_ACTIVITY_FEE if hasattr(settings,'US_MIN_SETTLEMENT_ACTIVITY_FEE') else 0.005), # 交易活动费0.01美元/笔，仅卖出收，相当于买卖各收0.005美元/笔
        ('max_settlement_activity_fee', settings.US_MAX_SETTLEMENT_ACTIVITY_FEE if hasattr(settings, 'US_MAX_SETTLEMENT_ACTIVITY_FEE') else 8.30),  # 交易活动费8.30美元/笔
        ('comprehensive_audit_supervision_fee', settings.US_COMPREHENSIVE_AUDIT_SUPERVISION_FEE if hasattr(settings, 'US_COMPREHENSIVE_AUDIT_SUPERVISION_FEE') else 0.0000265),  # 综合审计跟踪监管费0.0000265美元/笔
        ('slippage', settings.US_SLIPPAGE if hasattr(settings, 'US_SLIPPAGE') else 0.3),    #滑点0.3美元
    )

    def _getcommission(self, size, price, pseudoexec):
        value = abs(size) * price
        # 佣金计算
        commission = abs(size) * self.p.commission_per_share
        if commission < self.p.min_commission:
            commission = self.p.min_commission
        elif commission > value * self.p.max_commission_rate:
            commission = value * self.p.max_commission_rate

        trading_system_fee = abs(size) * self.p.min_trading_system_per_share
        if trading_system_fee < self.p.min_trading_system_fee:
            trading_system_fee = self.p.min_trading_system_fee
        elif trading_system_fee > value * self.p.max_trading_system_rate:
            trading_system_fee = value * self.p.max_trading_system_rate

        settlement_activity_fee = abs(size) * self.p.settlement_activity_fee_per_share
        if settlement_activity_fee < self.p.min_settlement_activity_fee:
            settlement_activity_fee = self.p.min_settlement_activity_fee
        elif settlement_activity_fee > self.p.max_settlement_activity_fee:
            settlement_activity_fee = self.p.max_settlement_activity_fee

        return commission + trading_system_fee + settlement_activity_fee + self.p.comprehensive_audit_supervision_fee
