import pandas as pd
import numpy as np
from datetime import datetime
from vnpy_ctastrategy import CtaTemplate, BarData
from vnpy.trader.utility import ArrayManager

class ModularDoubleMaStrategy(CtaTemplate):
    """模块化双均线策略 - 1分钟线模拟多周期过滤"""
    
    # 参数定义
    init_capital = 100000
    fast_window = 5      
    slow_window = 10     
    filter_n = 5         
    filter_fast = 5      
    filter_mid = 10      
    filter_slow = 20     
    atr_window = 14
    atr_multi = 3.0
    risk_percent = 0.01  
    
    # 变量定义
    long_stop = 0
    short_stop = 0
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.am = ArrayManager(size=150) 
        self.r_multiples = [] # 存储 {"datetime": str, "r_value": float}
        self.entry_price = 0
        self.initial_risk = 0
        self.strategy_logs = []

    def write_log(self, msg: str):
        super().write_log(msg)
        # 将日志保存到内存，方便报告读取
        self.strategy_logs.append(f"{datetime.now().strftime('%H:%M:%S')} - {msg}")

    def get_log(self):
        """供回测引擎调用"""
        return self.strategy_logs

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bar(10)

    def on_start(self):
        self.write_log("策略启动")

    def on_bar(self, bar: BarData):
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.check_exit_strategy(bar)
        
        if self.pos == 0:
            pre_cond = self.check_pre_condition()
            entry_cond = self.check_entry_condition()
            
            if pre_cond != 0 and entry_cond != 0:
                if pre_cond == entry_cond:
                    self.execute_open(bar, pre_cond)

    def check_pre_condition(self):
        ma_fast = self.am.sma(self.filter_fast * self.filter_n, array=False)
        ma_mid = self.am.sma(self.filter_mid * self.filter_n, array=False)
        ma_slow = self.am.sma(self.filter_slow * self.filter_n, array=False)   
        
        if ma_fast > ma_mid > ma_slow: return 1
        elif ma_fast < ma_mid < ma_slow: return -1
        return 0

    def check_entry_condition(self):
        ma_fast = self.am.sma(self.fast_window, array=False)
        ma_slow = self.am.sma(self.slow_window, array=False)
        # if len(ma_fast_array) < 2: return 0
        ma_fast_array = self.am.sma(self.fast_window, array=True)
        ma_slow_array = self.am.sma(self.slow_window, array=True)

        prev_ma_fast = ma_fast_array[-2]
        prev_ma_slow = ma_slow_array[-2] 
        
        if ma_fast > ma_slow and prev_ma_fast <= prev_ma_slow: return 1
        elif ma_fast < ma_slow and prev_ma_fast >= prev_ma_slow: return -1
        return 0

    def execute_open(self, bar: BarData, direction: int):
        atr = self.am.atr(self.atr_window, array=False)
        if atr == 0: return

        risk_amount = self.init_capital * self.risk_percent
        risk_per_share = atr * self.atr_multi
        size = int(risk_amount / risk_per_share)
        
        if size <= 0: return

        if direction == 1:
            self.buy(bar.close_price + 1, size)
            self.entry_price = bar.close_price
            self.initial_risk = risk_per_share
            self.long_stop = bar.close_price - self.initial_risk
            
        elif direction == -1:
            self.short(bar.close_price - 1, size)
            self.entry_price = bar.close_price
            self.initial_risk = risk_per_share
            self.short_stop = bar.close_price + self.initial_risk

    def check_exit_strategy(self, bar: BarData):
        atr = self.am.atr(self.atr_window, array=False)
        if self.pos > 0:
            self.long_stop = max(self.long_stop, bar.high_price - self.atr_multi * atr)
            if bar.close_price <= self.long_stop:
                self.sell(bar.close_price - 1, abs(self.pos))
                self.record_r_multiple(bar.close_price, bar.datetime)
                
        elif self.pos < 0:
            self.short_stop = min(self.short_stop, bar.low_price + self.atr_multi * atr)
            if bar.close_price >= self.short_stop:
                self.cover(bar.close_price + 1, abs(self.pos))
                self.record_r_multiple(bar.close_price, bar.datetime)

    def record_r_multiple(self, exit_price, dt):
        if self.initial_risk == 0: return
        profit = (exit_price - self.entry_price) if self.pos > 0 else (self.entry_price - exit_price)
        r_val = profit / self.initial_risk
        self.r_multiples.append({
            "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "r_value": round(r_val, 4)
        })