import os
import webbrowser
import numpy as np
import pandas as pd
import dolphindb as ddb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy_ctastrategy import BarData
from ModularDoubleMaStrategy import ModularDoubleMaStrategy

def get_dominant_period(symbol):
    """从 DolphinDB 获取主力合约期间"""
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")
    script = f"""
    t = loadTable("dfs://vnpy", "dominant_mapping")
    select min(trade_date) as start_date, max(trade_date) as end_date from t 
    where dominant_contract like '{symbol}%'
    """
    result = s.run(script)
    if result.empty: return None, None
    return result['start_date'][0], result['end_date'][0]

# --- 修改 create_web_report 函数 ---

import json
from datetime import date, datetime

def create_web_report(df: pd.DataFrame, stats: dict, target_symbol: str, initial_capital: float, size: float):
    # 1. 提取每日数据 (保持原有逻辑)
    daily_data = []
    for date_idx, row in df.iterrows():
        mkt_val = abs(row['end_pos']) * row['close_price'] * size
        daily_data.append({
            "date": date_idx.strftime('%Y-%m-%d'),
            "balance": round(row['balance'], 2),
            "drawdown": round(row['drawdown'], 2),
            "net_pnl": round(row['net_pnl'], 2),
            "pos": int(row['end_pos']),
            "price": round(row['close_price'], 2),
            "market_value": round(mkt_val, 2)
        })

# 2. 处理统计指标，将不可序列化的对象（日期、NumPy类型）转为原生 Python 类型
    processed_stats = {}
    for k, v in stats.items():
        # 转换日期/时间
        if isinstance(v, (date, datetime, pd.Timestamp)):
            processed_stats[k] = v.strftime('%Y-%m-%d')
        # 转换 NumPy 的整数 (如 int64)
        elif isinstance(v, (np.integer, np.int64)):
            processed_stats[k] = int(v)
        # 转换 NumPy 的浮点数 (如 float64) 或普通浮点数
        elif isinstance(v, (np.floating, np.float64, float)):
            if np.isnan(v) or np.isinf(v):
                processed_stats[k] = 0.0
            else:
                processed_stats[k] = float(v) # 强制转为原生 float
        else:
            processed_stats[k] = v

    # 3. 准备渲染映射
    render_mapping = {
        "{{target_symbol}}": str(target_symbol),
        "{{start_date}}": str(processed_stats.get('start_date', '-')),
        "{{end_date}}": str(processed_stats.get('end_date', '-')),
        "{{capital}}": f"{initial_capital:,.2f}",
        "{{initial_capital}}": str(initial_capital),
        "{{daily_json_data}}": json.dumps(daily_data),
        "{{stats_json_data}}": json.dumps(processed_stats) 
    }

    
    # 读取并替换模板
    with open("report_template.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    
    for key, value in render_mapping.items():
        html_content = html_content.replace(key, value)
    
    report_name = f"Report_{target_symbol}.html"
    with open(report_name, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f">>> 报告生成成功: {report_name}")
    webbrowser.open(f"file:///{os.path.abspath(report_name)}")


def run_backtest(target_symbol, db_path, table_name):
    """原有回测功能保持不变"""
    start_date, end_date = get_dominant_period(target_symbol)
    if start_date is None or pd.isna(start_date): return
    
    print(f">>> 开始运行 {target_symbol} 专业回测流程...")

    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")
    script = f"select * from loadTable('{db_path}', '{table_name}') where symbol=`{target_symbol}, datetime >= {start_date.strftime('%Y.%m.%d')}, datetime <= {end_date.strftime('%Y.%m.%d')}"
    data = s.run(script)
    
    history_data = [BarData(symbol=r['symbol'], exchange=Exchange(r['exchange']), datetime=r['datetime'], interval=Interval.MINUTE, 
                            open_price=r['open_price'], high_price=r['high_price'], low_price=r['low_price'], 
                            close_price=r['close_price'], volume=r['volume'], gateway_name="DB") for _, r in data.iterrows()]

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=f"{target_symbol}.LOCAL", interval=Interval.MINUTE, 
                          start=start_date.to_pydatetime() if hasattr(start_date, 'to_pydatetime') else start_date,
                          end=end_date.to_pydatetime() if hasattr(end_date, 'to_pydatetime') else end_date,
                          rate=0.0001, slippage=0.2, size=10, pricetick=1, capital=100000)
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    engine.history_data = history_data
    engine.run_backtesting()
    df = engine.calculate_result()
    statistics = engine.calculate_statistics()
    
    # 注入 R 倍数统计逻辑并生成报告
    # create_web_report(df, engine.strategy.r_multiples, stats, target_symbol)
    create_web_report(df, statistics, target_symbol, engine.capital, engine.size)
    return df, statistics # 可选

if __name__ == "__main__":
    run_backtest(target_symbol="AG2602", db_path="dfs://vnpy", table_name="bar")