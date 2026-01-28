import pandas as pd
import numpy as np
from datetime import datetime
from vnpy_ctastrategy import CtaTemplate, StopOrder, TickData, BarData
from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.utility import ArrayManager, BarGenerator
import dolphindb as ddb

class ModularDoubleMaStrategy(CtaTemplate):
    """模块化双均线策略"""
    
    # 参数定义
    init_capital = 100000
    fast_window = 5
    slow_window = 10
    filter_fast = 5
    filter_mid = 10
    filter_slow = 20
    atr_window = 14
    atr_multi = 3.0
    risk_percent = 0.01  # 每次亏损总资金的1%
    
    # 变量定义
    long_stop = 0
    short_stop = 0
    pos_size = 0
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        
        self.am = ArrayManager()
        # 创建5分钟K线生成器
        self.bg_5min = BarGenerator(self.on_bar, 5, self.on_5min_bar)
        self.am_5min = ArrayManager()
        
        # 记录每笔交易的R倍数，用于后期统计
        self.r_multiples = []
        self.entry_price = 0
        self.initial_risk = 0

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bar(10)

    def on_start(self):
        self.write_log("策略启动")

    def on_bar(self, bar: BarData):
        """1分钟K线推送"""
        self.bg_5min.update_bar(bar) # 合成5分钟线
        self.am.update_bar(bar)
        
        if not self.am.inited or not self.am_5min.inited:
            return

        # 执行离场逻辑 (止损/止盈)
        self.check_exit_strategy(bar)
        
        # 执行进场逻辑
        if self.pos == 0:
            if self.check_pre_condition() and self.check_entry_condition():
                self.execute_open(bar)

    def on_5min_bar(self, bar: BarData):
        """5分钟K线更新"""
        self.am_5min.update_bar(bar)

    # --- 模块化部分 ---

    def check_pre_condition(self):
        """1) 进场前提条件：5分钟级别趋势过滤"""
        ma_fast = self.am_5min.sma(self.filter_fast)
        ma_mid = self.am_5min.sma(self.filter_mid)
        ma_slow = self.am_5min.sma(self.filter_slow)
        
        if ma_fast > ma_mid > ma_slow:
            return 1 # 多头排列
        elif ma_fast < ma_mid < ma_slow:
            return -1 # 空头排列
        return 0

    def check_entry_condition(self):
        """2) 具体进场策略：1分钟双均线金叉死叉"""
        ma_fast = self.am.sma(self.fast_window)
        ma_slow = self.am.sma(self.slow_window)
        
        if ma_fast > ma_slow and self.am.sma(self.fast_window, 1) <= self.am.sma(self.slow_window, 1):
            return 1 # 金叉
        elif ma_fast < ma_slow and self.am.sma(self.fast_window, 1) >= self.am.sma(self.slow_window, 1):
            return -1 # 死叉
        return 0

    def calculate_risk_position(self, price, atr):
        """5) 仓位大小管理：基于R的风险控制"""
        # 风险额度 = 当前净值 * 1%
        # 此处简单使用初始资金，实际回测中可用 engine.get_portfolio_value()
        risk_amount = self.init_capital * self.risk_percent
        risk_per_share = atr * self.atr_multi
        
        if risk_per_share == 0: return 0
        return int(risk_amount / risk_per_share)

    def execute_open(self, bar: BarData):
        """模块化开仓"""
        pre_cond = self.check_pre_condition()
        entry_cond = self.check_entry_condition()
        atr = self.am.atr(self.atr_window)
        
        size = self.calculate_risk_position(bar.close_price, atr)
        if size <= 0: return

        if pre_cond == 1 and entry_cond == 1:
            self.buy(bar.close_price + 0.5, size)
            self.entry_price = bar.close_price
            self.initial_risk = atr * self.atr_multi
            self.long_stop = bar.close_price - self.initial_risk
            
        elif pre_cond == -1 and entry_cond == -1:
            self.short(bar.close_price - 0.5, size)
            self.entry_price = bar.close_price
            self.initial_risk = atr * self.atr_multi
            self.short_stop = bar.close_price + self.initial_risk

    def check_exit_strategy(self, bar: BarData):
        """3) & 4) 离场策略：初始止损R + 吊灯止损"""
        if self.pos > 0:
            # 吊灯止损逻辑（简化版：追踪最高价回撤）
            atr = self.am.atr(self.atr_window)
            self.long_stop = max(self.long_stop, bar.high_price - self.atr_multi * atr)
            
            if bar.close_price <= self.long_stop:
                self.sell(bar.close_price - 0.5, abs(self.pos))
                self.record_r_multiple(bar.close_price)
                
        elif self.pos < 0:
            atr = self.am.atr(self.atr_window)
            self.short_stop = min(self.short_stop, bar.low_price + self.atr_multi * atr)
            
            if bar.close_price >= self.short_stop:
                self.cover(bar.close_price + 0.5, abs(self.pos))
                self.record_r_multiple(bar.close_price)

    def record_r_multiple(self, exit_price):
        """记录R倍数"""
        if self.initial_risk == 0: return
        profit = (exit_price - self.entry_price) if self.pos > 0 else (self.entry_price - exit_price)
        r_val = profit / self.initial_risk
        self.r_multiples.append(r_val)