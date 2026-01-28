import pandas as pd
import numpy as np
from datetime import datetime
from vnpy_ctastrategy import CtaTemplate, StopOrder, TickData, BarData
from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.utility import ArrayManager
import dolphindb as ddb

class ModularDoubleMaStrategy(CtaTemplate):
    """模块化双均线策略 - 1分钟线模拟多周期过滤"""
    
    # 参数定义
    init_capital = 100000
    fast_window = 5      # 1分钟快线
    slow_window = 10     # 1分钟慢线
    
    filter_n = 5         # 模拟倍数（5代表模拟5分钟级别趋势）
    filter_fast = 5      # 模拟5min的5均线 -> 1min的25均线
    filter_mid = 10      # 模拟5min的10均线 -> 1min的50均线
    filter_slow = 20     # 模拟5min的20均线 -> 1min的100均线
    
    atr_window = 14
    atr_multi = 3.0
    risk_percent = 0.01  # 每次亏损总资金的1%
    
    # 变量定义
    long_stop = 0
    short_stop = 0
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        # ArrayManager容量需大于最长均线周期 (20 * 5 = 100)
        self.am = ArrayManager(size=150) 
        
        self.r_multiples = []
        self.entry_price = 0
        self.initial_risk = 0

    def on_init(self):
        self.write_log("策略初始化")
        # 预热 10 天的数据。即使 history_data 是手动注入的，load_bar 也会从注入的数据中提取前 10 天的内容以确保 am.inited 变为 True
        self.load_bar(10)

    def on_start(self):
        self.write_log("策略启动")

    def on_bar(self, bar: BarData):
        """1分钟K线推送"""
        self.am.update_bar(bar)

        # 检查初始化状态
        if not self.am.inited:
            return

        # 执行离场逻辑
        self.check_exit_strategy(bar)
        
        # 执行进场逻辑
        if self.pos == 0:
            pre_cond = self.check_pre_condition()
            entry_cond = self.check_entry_condition()
            
            if pre_cond != 0 and entry_cond != 0:
                # 只有方向一致时才开仓
                if pre_cond == entry_cond:
                    self.execute_open(bar, pre_cond)

    def check_pre_condition(self):
        """1) 趋势过滤：利用1分钟线模拟N分钟级别均线排列"""
        # 计算模拟的长周期均线
        ma_fast = self.am.sma(self.filter_fast * self.filter_n, array=False)
        ma_mid = self.am.sma(self.filter_mid * self.filter_n, array=False)
        ma_slow = self.am.sma(self.filter_slow * self.filter_n, array=False)   
        
        if ma_fast > ma_mid > ma_slow:
            return 1  # 多头排列
        elif ma_fast < ma_mid < ma_slow:
            return -1 # 空头排列
        return 0

    def check_entry_condition(self):
        """2) 进场信号：1分钟双均线金叉死叉"""
        ma_fast = self.am.sma(self.fast_window, array=False)
        ma_slow = self.am.sma(self.slow_window, array=False)
        
        # 上一根 K 线的均线值 (使用 sma_array 再取索引，或者通过逻辑获取)
        # 最简单的做法是取回整个数组再用索引 [-2] 取倒数第二位
        ma_fast_array = self.am.sma(self.fast_window, array=True)
        ma_slow_array = self.am.sma(self.slow_window, array=True)

        prev_ma_fast = ma_fast_array[-2]
        prev_ma_slow = ma_slow_array[-2] 
        
        if ma_fast > ma_slow and prev_ma_fast <= prev_ma_slow:
            return 1  # 金叉
        elif ma_fast < ma_slow and prev_ma_fast >= prev_ma_slow:
            return -1 # 死叉
        return 0

    def execute_open(self, bar: BarData, direction: int):
        """模块化开仓"""
        atr = self.am.atr(self.atr_window, array=False)
        if atr == 0: return

        # 计算仓位
        risk_amount = self.init_capital * self.risk_percent
        risk_per_share = atr * self.atr_multi
        size = int(risk_amount / risk_per_share)
        
        if size <= 0: return

        if direction == 1:
            self.buy(bar.close_price + 1, size)
            self.entry_price = bar.close_price
            self.initial_risk = risk_per_share
            self.long_stop = bar.close_price - self.initial_risk
            self.write_log(f"做多开仓: {size}股, 价格: {bar.close_price}, 止损: {self.long_stop}")
            
        elif direction == -1:
            self.short(bar.close_price - 1, size)
            self.entry_price = bar.close_price
            self.initial_risk = risk_per_share
            self.short_stop = bar.close_price + self.initial_risk
            self.write_log(f"做空开仓: {size}股, 价格: {bar.close_price}, 止损: {self.short_stop}")

    def check_exit_strategy(self, bar: BarData):
        """3) & 4) 离场策略：初始止损 + 追踪止损"""
        atr = self.am.atr(self.atr_window, array=False)
        
        if self.pos > 0:
            # 追踪最高价回撤止损
            self.long_stop = max(self.long_stop, bar.high_price - self.atr_multi * atr)
            if bar.close_price <= self.long_stop:
                self.sell(bar.close_price - 1, abs(self.pos))
                self.record_r_multiple(bar.close_price)
                
        elif self.pos < 0:
            # 追踪最低价回撤止损
            self.short_stop = min(self.short_stop, bar.low_price + self.atr_multi * atr)
            if bar.close_price >= self.short_stop:
                self.cover(bar.close_price + 1, abs(self.pos))
                self.record_r_multiple(bar.close_price)

    def record_r_multiple(self, exit_price):
        """记录R倍数统计"""
        if self.initial_risk == 0: return
        profit = (exit_price - self.entry_price) if self.pos > 0 else (self.entry_price - exit_price)
        r_val = profit / self.initial_risk
        self.r_multiples.append(r_val)