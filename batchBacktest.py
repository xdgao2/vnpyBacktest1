import pandas as pd
import numpy as np
import dolphindb as ddb
from datetime import datetime
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval

# 这里导入上一步定义的 ModularDoubleMaStrategy 类
# from your_strategy_file import ModularDoubleMaStrategy 

def run_continuous_backtest(product_code="AG", init_capital=100000):
    """
    针对某一品种（如AG），自动寻找历史上所有主力合约时段并连续回测
    """
    # 1. 加载映射表
    mapping_path = r"D:\Data\FutureData\futures_dominant_mapping.csv"
    df_map = pd.read_csv(mapping_path, index_col=0, parse_dates=True)
    
    if product_code not in df_map.columns:
        print(f"品种 {product_code} 不在映射表中")
        return

    # 提取该品种的主力合约序列，并找出切换点
    series = df_map[product_code].dropna()
    # 找出合约发生变化的行（即主力切换点）
    change_mask = series != series.shift(1)
    dominant_segments = []
    
    symbols = series[change_mask].values
    start_dates = series[change_mask].index
    end_dates = list(start_dates[1:]) + [series.index[-1]]
    
    for sym, start, end in zip(symbols, start_dates, end_dates):
        # 去掉 vnpy 不识别的后缀（如 AG2005.SHFE -> AG2005）
        clean_sym = sym.split('.')[0]
        dominant_segments.append({
            "symbol": clean_sym,
            "start": start,
            "end": end
        })

    # 2. 连接 DolphinDB
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")
    
    all_history_data = []
    print(f"开始加载 {product_code} 的历史数据...")

    for seg in dominant_segments:
        script = f"""
        t = loadTable("dfs://vnpy", "bar")
        select * from t where symbol=`{seg['symbol']}, 
        datetime >= {seg['start'].strftime('%Y.%m.%d')}, 
        datetime <= {seg['end'].strftime('%Y.%m.%d')}
        """
        data = s.run(script)
        
        # 转换为 vn.py BarData
        for _, row in data.iterrows():
            bar = BarData(
                symbol=row['symbol'],
                exchange=None,
                datetime=row['datetime'],
                interval=Interval.MINUTE,
                open_price=row['open'],
                high_price=row['high'],
                low_price=row['low'],
                close_price=row['close'],
                volume=row['volume'],
                gateway_name="DDB"
            )
            all_history_data.append(bar)

    # 3. 配置回测引擎
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=f"{product_code}_Continuous.LOCAL",
        interval=Interval.MINUTE,
        start=dominant_segments[0]['start'],
        end=dominant_segments[-1]['end'],
        rate=0.0001,
        slippage=0.2,
        size=15,       # 白银一手15千克，此处需根据品种调整
        pricetick=1,
        capital=init_capital
    )
    
    # 添加策略
    engine.add_strategy(ModularDoubleMaStrategy, {})
    engine.history_data = all_history_data
    
    # 4. 执行回测
    print("正在运行回测逻辑...")
    engine.run_backtesting()
    engine.calculate_result()
    stats = engine.calculate_statistics()
    
    # 5. Van Tharp 深度统计输出
    print_van_tharp_stats(engine, stats)

def print_van_tharp_stats(engine, stats):
    """文本化输出核心绩效指标"""
    r_multiples = engine.strategy.r_multiples
    
    if not r_multiples:
        print("回测期内无成交记录")
        return

    # 计算指标
    win_rate = len([r for r in r_multiples if r > 0]) / len(r_multiples)
    avg_r = np.mean(r_multiples)
    std_r = np.std(r_multiples)
    total_r = sum(r_multiples)
    # SQN公式
    sqn = (len(r_multiples)**0.5) * (avg_r / std_r) if std_r != 0 else 0
    
    # 最大回撤(R视角)
    cum_r = np.cumsum(r_multiples)
    max_r = np.maximum.accumulate(cum_r)
    drawdown_r = max_r - cum_r
    max_drawdown_r = np.max(drawdown_r)

    print("\n" + "="*40)
    print(f"【策略绩效报告 - {engine.vt_symbol}】")
    print("-" * 40)
    print(f"1. 基础指标:")
    print(f"   起始资金: {stats['capital']:,.2f}")
    print(f"   最终净值: {stats['end_balance']:,.2f}")
    print(f"   总收益率: {stats['total_return']:.2%}")
    print(f"   最大回撤: {stats['max_drawdown']:.2f} ({stats['max_ddpercent']:.2f}%)")
    print(f"   夏普比率: {stats['sharpe_ratio']:.2f}")
    
    print(f"\n2. Van Tharp 核心指标:")
    print(f"   总操作次数: {len(r_multiples)}")
    print(f"   胜率 (Win Rate): {win_rate:.2%}")
    print(f"   期望值 (Avg R): {avg_r:.2f}R")
    print(f"   收益稳定性 (StdDev R): {std_r:.2f}")
    print(f"   系统质量数 (SQN): {sqn:.2f}")
    print(f"   总R收益: {total_r:.2f}R")
    print(f"   最大R回撤: {max_drawdown_r:.2f}R")
    
    print(f"\n3. 时间维度:")
    print(f"   回测天数: {stats['total_days']}")
    print(f"   月均操作: {(len(r_multiples) / stats['total_days'] * 30):.1f} 次")
    print("="*40 + "\n")

# 执行 AG 品种的连续回测
if __name__ == "__main__":
    run_continuous_backtest("AG")
    # 将每日净值数据保存到本地
    df.to_csv("backtest_result.csv")

    # 将所有的成交记录保存
    trades = engine.get_all_trades()
    import pandas as pd
    pd.DataFrame([t.__dict__ for t in trades]).to_csv("trades.csv")