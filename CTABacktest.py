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

# vnpy 依赖
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.utility import ArrayManager
from vnpy_ctastrategy import BarData
from ModularDoubleMaStrategy import ModularDoubleMaStrategy

# ==========================================
# 数据清洗与 JSON 编码 (确保无 NaN/Inf)
# ==========================================
class DataSanitizer:
    @staticmethod
    def clean(obj: Any) -> Any:
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32, float)):
            if np.isnan(obj) or np.isinf(obj): return 0.0
            return round(float(obj), 4)
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, dict):
            return {k: DataSanitizer.clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, np.ndarray)):
            return [DataSanitizer.clean(i) for i in obj]
        if hasattr(obj, "value"): # 处理 Direction, Offset 枚举
            return str(obj.value)
        return obj

# ==========================================
# 回测引擎逻辑
# ==========================================
def run_backtest(target_symbol: str):
    # 1. 从 DolphinDB 获取时间范围
    s = ddb.session()
    try:
        s.connect("localhost", 8848, "admin", "123456")
    except Exception as e:
        print(f"DolphinDB 连接失败: {e}")
        return

    script_period = f"""
    t = loadTable("dfs://vnpy", "dominant_mapping")
    select min(trade_date) as start_date, max(trade_date) as end_date from t 
    where dominant_contract like '{target_symbol}%'
    """
    period_df = s.run(script_period)
    if period_df.empty or pd.isna(period_df['start_date'][0]):
        print(f"未能在映射表中找到品种 {target_symbol} 的主力期间")
        return

    start_date = period_df['start_date'][0]
    end_date = period_df['end_date'][0]
    print(f"主力期间确认: {start_date} 至 {end_date}")

    # 2. 从 DolphinDB 加载 K 线数据
    # 注意：这里根据你的逻辑从 dfs://vnpy/bars 加载数据
    script_data = f"""
    t = loadTable("dfs://vnpy", "bar")
    select * from t where symbol like '{target_symbol}%', 
    datetime >= {start_date.strftime('%Y.%m.%d')}, 
    datetime <= {end_date.strftime('%Y.%m.%d')}
    order by datetime
    """
    bar_df = s.run(script_data)
    if bar_df.empty:
        print("查询到的 K 线数据为空，请检查 bar 表")
        return

    history_data = [
        BarData(
            symbol=r['symbol'],
            exchange=Exchange(r['exchange']),
            datetime=r['datetime'],
            interval=Interval.MINUTE,
            open_price=r['open_price'],
            high_price=r['high_price'],
            low_price=r['low_price'],
            close_price=r['close_price'],
            volume=r['volume'],
            gateway_name="DB"
        ) for _, r in bar_df.iterrows()
    ]
    print(f"成功加载数据量: {len(history_data)}")

    # 3. 配置引擎
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=f"{target_symbol}.LOCAL",
        interval=Interval.MINUTE,
        start=start_date.to_pydatetime() if hasattr(start_date, 'to_pydatetime') else start_date,
        end=end_date.to_pydatetime() if hasattr(end_date, 'to_pydatetime') else end_date,
        rate=0.0001,
        slippage=0.2,
        size=10,
        pricetick=1,
        capital=100000
    )
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    engine.history_data = history_data  # 直接注入数据
    
    # 4. 运行回测
    engine.run_backtesting()
    df_daily = engine.calculate_result()
    stats = engine.calculate_statistics()

    # 5. 终端输出
    print("\n" + "="*50)
    print(f"策略回测统计: {target_symbol}")
    metrics = ["total_return", "annual_return", "max_drawdown", "sharpe_ratio", "winning_rate"]
    for m in metrics:
        print(f"{m:<20}: {stats.get(m, 0):.2f}")
    print("="*50)

    # 6. 数据打包
    data_packet = {
        "metadata": {
            "strategy": "ModularDoubleMaStrategy",
            "symbol": target_symbol,
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d"),
            "capital": 100000
        },
        "stats": stats,
        "daily_data": [
            {
                "date": d.strftime("%Y-%m-%d"),
                "balance": float(r['balance']),
                "net_pnl": float(r['net_pnl']),
                "drawdown": float(r['drawdown'])
            } for d, r in df_daily.iterrows()
        ],
        "trades": [
            {
                "datetime": t.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "direction": t.direction.value,
                "offset": t.offset.value,
                "price": t.price,
                "volume": t.volume
            } for t in engine.get_all_trades()
        ],
        "klines": [
            [b.datetime.strftime("%Y-%m-%d %H:%M:%S"), b.open_price, b.close_price, b.low_price, b.high_price, b.volume]
            for b in history_data
        ],
        # "logs": [f"{log}" for log in engine.strategy.get_log() if hasattr(engine.strategy, 'get_log')] if hasattr(engine, 'strategy') else []
    }

    # 7. 保存文件并导出
    output_dir = Path("backtest_results") / f"Report_{target_symbol}_{datetime.now().strftime('%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    cleaned_data = DataSanitizer.clean(data_packet)
    with open(output_dir / "data.js", "w", encoding="utf-8") as f:
        f.write(f"window.BACKTEST_DATA = {json.dumps(cleaned_data, ensure_ascii=False)};")
    
    # 拷贝模板 (确保 report_template.html 在当前目录)
    shutil.copy("report_template.html", output_dir / "report.html")
    
    print(f"报告已生成至: {output_dir}")
    webbrowser.open((output_dir / "report.html").absolute().as_uri())

if __name__ == "__main__":
    run_backtest("AG2602")