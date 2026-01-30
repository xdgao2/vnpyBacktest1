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
    position_limit = 100  # 仓位上限  
    
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

    def calculate_position_size(self, atr):
        """计算仓位大小
        
        Args:
            atr: 平均真实波动幅度
            
        Returns:
            int: 计算后的仓位大小
        """
        if atr == 0:
            return 0
        
        # 获取当前总资金
        # 在vnpy中，可以通过cta_engine获取当前资金
        # 但为了简化，我们可以使用一个变量来跟踪当前资金
        # 这里假设cta_engine有get_all_positions方法来获取当前资金
        # 注意：不同版本的vnpy可能有不同的获取资金的方式
        # 这里使用一种通用的方法
        try:
            # 尝试获取当前总资金
            if hasattr(self, 'cta_engine') and hasattr(self.cta_engine, 'get_all_positions'):
                # 这种方式适用于某些vnpy版本
                current_capital = self.cta_engine.get_all_positions().get('capital', self.init_capital)
            elif hasattr(self, 'capital'):
                # 这种方式适用于某些自定义回测引擎
                current_capital = self.capital
            else:
                # 如果无法获取当前资金，使用初始资金作为备选
                current_capital = self.init_capital
        except:
            # 如果出现异常，使用初始资金
            current_capital = self.init_capital
        
        # 根据Van Tharp的1%风险模型计算仓位，使用当前总资金
        risk_amount = current_capital * self.risk_percent
        risk_per_share = atr * self.atr_multi
        
        # 计算理论仓位大小
        theoretical_size = int(risk_amount / risk_per_share)
        
        # 应用仓位上限
        position_size = min(theoretical_size, self.position_limit)
        
        return max(position_size, 0)

    def execute_open(self, bar: BarData, direction: int):
        atr = self.am.atr(self.atr_window, array=False)
        if atr == 0: return

        size = self.calculate_position_size(atr)
        
        if size <= 0: return

        # 重新计算风险参数，因为calculate_position_size方法中没有返回这个值
        risk_per_share = atr * self.atr_multi

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