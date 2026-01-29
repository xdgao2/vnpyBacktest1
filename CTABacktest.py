import os
import shutil
import json
import webbrowser
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval
from ModularDoubleMaStrategy import ModularDoubleMaStrategy

class BacktestReportGenerator:
    def __init__(self, engine: BacktestingEngine, target_symbol: str, strategy_name: str):
        self.engine = engine
        self.symbol = target_symbol
        self.strategy_name = strategy_name
        self.timestamp = datetime.now()
        
    def _clean_data(self, obj):
        """递归清理数据，确保 JSON 序列化友好"""
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32)):
            return 0 if (np.isnan(obj) or np.isinf(obj)) else round(float(obj), 4)
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, dict):
            return {k: self._clean_data(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_data(i) for i in obj]
        return obj

    def prepare_data(self):
        """提取所有回测数据"""
        df_daily = self.engine.calculate_result()
        stats = self.engine.calculate_statistics()
        trades = self.engine.get_all_trades()
        orders = self.engine.get_all_orders()
        history = self.engine.history_data  # 获取K线数据用于信号分析
        logs = self.engine.logs if hasattr(self.engine, 'logs') else []

        # 1. 每日数据处理
        daily_records = []
        if not df_daily.empty:
            df_daily['date'] = df_daily.index
            # 确保包含 UI 需要的字段
            daily_records = df_daily.reset_index(drop=True).to_dict('records')

        # 2. 交易记录处理
        trade_records = []
        for t in trades:
            trade_records.append({
                "datetime": t.datetime,
                "symbol": t.symbol,
                "direction": t.direction.value, # "多"/"空"
                "offset": t.offset.value,       # "开"/"平"
                "price": t.price,
                "volume": t.volume
            })
            
        # 3. K线数据 (为了前端性能，如果是分钟线，数据量大时可能需要抽样，这里全量输出)
        kline_records = []
        for bar in history:
            kline_records.append([
                bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                bar.open_price,
                bar.close_price,
                bar.low_price,
                bar.high_price,
                bar.volume
            ])

        # 4. 组装总数据包
        return {
            "metadata": {
                "strategy": self.strategy_name,
                "symbol": self.symbol,
                "start_date": self.engine.start.strftime("%Y-%m-%d"),
                "end_date": self.engine.end.strftime("%Y-%m-%d"),
                "capital": self.engine.capital,
                "run_time": self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            },
            "stats": stats,
            "daily_data": daily_records,
            "trades": trade_records,
            "klines": kline_records,
            "logs": logs
        }

    def save_and_show(self):
        """生成文件并展示"""
        # 1. 创建目录: 策略名_品种_日期_时间
        folder_name = f"{self.strategy_name}_{self.symbol}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}"
        base_path = Path.cwd() / "backtest_results" / folder_name
        base_path.mkdir(parents=True, exist_ok=True)

        data = self.prepare_data()
        cleaned_data = self._clean_data(data)

        # 2. 保存数据文件 (data.js)
        # 技巧：保存为 JS 变量而非 JSON，避免本地文件跨域(CORS)问题
        js_content = f"window.BACKTEST_DATA = {json.dumps(cleaned_data, ensure_ascii=False)};"
        with open(base_path / "data.js", "w", encoding="utf-8") as f:
            f.write(js_content)

        # 3. 复制 HTML 模板
        template_path = Path.cwd() / "report_template.html"
        target_html = base_path / "report.html"
        
        if not template_path.exists():
            print("错误：找不到 report_template.html 模板文件")
            return
            
        shutil.copy(template_path, target_html)

        print(f"回测报告已生成: {target_html}")
        webbrowser.open(target_html.as_uri())

def run_backtest(target_symbol: str):
    engine = BacktestingEngine()
    
    # 请确保已注入数据或连接数据库
    # engine.set_parameters(...)
    # 这里模拟设置，实际使用请替换为真实数据加载逻辑
    engine.set_parameters(
        vt_symbol=f"{target_symbol}.LOCAL",
        interval=Interval.MINUTE,
        start=datetime(2025, 1, 1),
        end=datetime(2026, 1, 1),
        rate=0.0001, slippage=0.2, size=10, pricetick=1, capital=100000
    )
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    
    # 模拟运行 (实际应调用 engine.run_backtesting())
    # 为了演示，这里假设已经运行完毕，history_data 已存在
    # 如果没有真实数据，下面会报错。请确保 load_data 可用。
    print("开始回测...")
    engine.load_data() 
    engine.run_backtesting()
    
    # 生成报告
    reporter = BacktestReportGenerator(engine, target_symbol, "ModularDoubleMaStrategy")
    reporter.save_and_show()

if __name__ == "__main__":
    # 确保当前目录下有 report_template.html
    run_backtest("AG2602")