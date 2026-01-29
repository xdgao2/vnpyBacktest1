import os
import webbrowser
import numpy as np
import pandas as pd
import dolphindb as ddb
import json
from datetime import datetime, date
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy_ctastrategy import BarData
from ModularDoubleMaStrategy import ModularDoubleMaStrategy

# 自定义 JSON 编码器，解决 Numpy 类型和特殊数值问题
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32)):
            if np.isnan(obj) or np.isinf(obj): return 0  # 核心修复：防止非法数值导致JS崩溃
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.strftime("%Y-%m-%d")
        return super(NpEncoder, self).default(obj)

def clean_datetime(dt):
    if dt is None: return None
    if isinstance(dt, pd.Timestamp): return dt.to_pydatetime()
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return datetime.combine(dt, datetime.min.time())
    return dt

def create_web_report(df: pd.DataFrame, stats: dict, target_symbol: str):
    """生成包含分页功能的 HTML 回测报告"""
    
    # 1. 数据清洗与准备
    daily_data = []
    df = df.fillna(0)
    for date_idx, row in df.iterrows():
        daily_data.append({
            "date": date_idx.strftime("%Y-%m-%d"),
            "balance": round(float(row['balance']), 2),
            "drawdown": round(float(row['drawdown']), 2),
            "net_pnl": round(float(row['net_pnl']), 2),
            "pos": int(row['net_pos']),
            "close": round(float(row['close_price']), 2) if 'close_price' in row else 0
        })

    # 2. 包装 JSON 数据
    report_payload = {
        "metadata": {
            "symbol": target_symbol,
            "strategy": "ModularDoubleMaStrategy",
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "stats": stats,
        "daily_data": daily_data
    }
    
    json_str = json.dumps(report_payload, cls=NpEncoder, ensure_ascii=False)

    # 3. 读取模板并替换 (确保 template 文件在同级目录)
    with open("report_template.html", "r", encoding="utf-8") as f:
        template = f.read()
    
    final_html = template.replace("{{backtest_data}}", json_str)
    final_html = final_html.replace("{{target_symbol}}", target_symbol)

    # 4. 存储与自动打开
    if not os.path.exists("backtest_results"):
        os.makedirs("backtest_results")
        
    file_path = f"backtest_results/Report_{target_symbol}_{datetime.now().strftime('%m%d_%H%M%S')}.html"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(final_html)
    
    print(f"\n[OK] 报告生成完毕: {file_path}")
    webbrowser.open("file://" + os.path.abspath(file_path))

def run_backtest(target_symbol: str):
    # 这里接入你的 DolphinDB 获取数据逻辑，为了演示生成 history_data
    # history_data = [...] 
    
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=f"{target_symbol}.LOCAL",
        interval=Interval.MINUTE,
        start=datetime(2025, 1, 1),
        end=datetime(2026, 1, 1),
        rate=0.0001, slippage=0.2, size=10, pricetick=1, capital=100000
    )
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    # engine.run_backtesting() ... 
    
    df = engine.calculate_result()
    stats = engine.calculate_statistics()
    
    create_web_report(df, stats, target_symbol)

if __name__ == "__main__":
    run_backtest("AG2602")