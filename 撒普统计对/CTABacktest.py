import os
import shutil
import json
import webbrowser
import numpy as np
import pandas as pd
import dolphindb as ddb
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List

# 完整补齐 vnpy 相关导入
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy_ctastrategy import BarData
from ModularDoubleMaStrategy import ModularDoubleMaStrategy

# 数据清洗工具类
class DataSanitizer:
    @staticmethod
    def clean(obj: Any) -> Any:
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)): return int(obj)
        if isinstance(obj, (np.float64, np.float32, float)):
            if np.isnan(obj) or np.isinf(obj): return 0.0
            return round(float(obj), 4)
        if isinstance(obj, (datetime, date, pd.Timestamp)): return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, dict): return {k: DataSanitizer.clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, np.ndarray)): return [DataSanitizer.clean(i) for i in obj]
        return obj

def run_backtest(target_symbol: str):
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")

    # 获取回测区间
    script_period = f"t = loadTable('dfs://vnpy', 'dominant_mapping'); select min(trade_date) as start_date, max(trade_date) as end_date from t where dominant_contract like '{target_symbol}%'"
    period_df = s.run(script_period)
    
    # 【修复重点】：强制转换为 Python 原生 datetime，解决 SQLite 报错
    start_date = period_df['start_date'][0].to_pydatetime()
    end_date = period_df['end_date'][0].to_pydatetime()

    # 获取 K 线数据
    script_data = f"t = loadTable('dfs://vnpy', 'bar'); select * from t where symbol like '{target_symbol}%', datetime >= {start_date.strftime('%Y.%m.%d')}, datetime <= {end_date.strftime('%Y.%m.%d')} order by datetime"
    bar_df = s.run(script_data)
    
    # 【修复重点】：BarData 构造函数中的 datetime 也需转换
    history_data = [
        BarData(
            symbol=r['symbol'], 
            exchange=Exchange(r['exchange']), 
            datetime=r['datetime'].to_pydatetime(), 
            interval=Interval.MINUTE, 
            open_price=r['open_price'], 
            high_price=r['high_price'], 
            low_price=r['low_price'], 
            close_price=r['close_price'], 
            volume=r['volume'], 
            gateway_name="DB"
        ) for _, r in bar_df.iterrows()
    ]

    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=f"{target_symbol}.LOCAL", 
        interval=Interval.MINUTE,
        start=start_date, 
        end=end_date,
        rate=0.0001, slippage=0.2, size=10, pricetick=1, capital=100000
    )
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    engine.history_data = history_data  
    engine.run_backtesting()
    
    df_daily = engine.calculate_result()
    stats = engine.calculate_statistics()
    
    # 获取 R 倍数数据并计算 Tharp 指标
    strategy_inst = engine.strategy
    r_multiples = strategy_inst.r_multiples
    tharp_stats = {}
    if r_multiples:
        r_vals = [x['r_value'] for x in r_multiples]
        tharp_stats['win_rate'] = len([r for r in r_vals if r > 0]) / len(r_vals) * 100
        tharp_stats['mean_r'] = np.mean(r_vals)
        tharp_stats['std_r'] = np.std(r_vals)
        tharp_stats['sqn'] = np.sqrt(len(r_vals)) * (tharp_stats['mean_r'] / tharp_stats['std_r']) if tharp_stats['std_r'] != 0 else 0
        tharp_stats['total_r'] = np.sum(r_vals)

        print("R乘数：", r_multiples, tharp_stats)

    # 打包所有数据发送给 HTML
    data_packet = {
        "metadata": {"strategy": "ModularDoubleMaStrategy", "symbol": target_symbol, "start": start_date, "end": end_date},
        "stats": stats,
        "tharp_stats": tharp_stats,
        "r_data": r_multiples,
        "daily_data": [{"date": d.strftime("%Y-%m-%d"), "balance": r['balance'], "net_pnl": r['net_pnl'], "drawdown": r['drawdown']} for d, r in df_daily.iterrows()],
        "trades": [{"datetime": t.datetime.strftime("%Y-%m-%d %H:%M:%S"), "direction": t.direction.value, "offset": t.offset.value, "price": t.price, "volume": t.volume} for t in engine.get_all_trades()],
        "klines": [[b.datetime.strftime("%Y-%m-%d %H:%M:%S"), b.open_price, b.close_price, b.low_price, b.high_price, b.volume] for b in history_data],
        "logs": strategy_inst.get_log()
    }

    # 创建报告目录
    now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path("backtest_results") / f"Report_{target_symbol}_{now_str}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 导出 data.js
    with open(output_dir / "data.js", "w", encoding="utf-8") as f:
        f.write(f"window.BACKTEST_DATA = {json.dumps(DataSanitizer.clean(data_packet), ensure_ascii=False)};")
    
    # 复制模板
    shutil.copy("report_template.html", output_dir / "report.html")
    webbrowser.open((output_dir / "report.html").absolute().as_uri())

if __name__ == "__main__":
    run_backtest("AG2602")