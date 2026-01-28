from vnpy_ctastrategy.backtesting import BacktestingEngine
import pandas as pd

def run_backtest(target_symbol="AG2005", db_path="dfs://vnpy", table_name="bar"):
    # 1. 处理主力合约映射
    mapping_path = r"D:\Data\FutureData\futures_dominant_mapping.csv"
    df_mapping = pd.read_csv(mapping_path, index_col=0, parse_dates=True)
    
    # 获取该品种对应的列（例如 'AG'）
    product_code = ''.join([i for i in target_symbol if not i.isdigit()]).upper()
    if product_code not in df_mapping.columns:
        print(f"找不到品种 {product_code}")
        return

    # 找到该合约作为主力的日期范围
    series = df_mapping[product_code]
    # 注意：CSV中的格式可能是 AG2005.SHFE，需做匹配
    dominant_dates = series[series.str.contains(target_symbol, na=False)].index
    
    if dominant_dates.empty:
        print(f"合约 {target_symbol} 在映射表中未被识别为主力合约")
        return
    
    start_date = dominant_dates.min()
    end_date = dominant_dates.max()
    
    print(f"回测合约: {target_symbol} | 主力期间: {start_date} 至 {end_date}")

    # 2. 从 DolphinDB 加载数据
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456") # 根据实际修改
    
    # 构造DolphinDB脚本查询数据
    # 过滤时间范围和合约代码
    script = f"""
    t = loadTable("{db_path}", "{table_name}")
    select * from t where symbol=`{target_symbol}, 
    datetime >= {start_date.strftime('%Y.%m.%d')}, 
    datetime <= {end_date.strftime('%Y.%m.%d')}
    """
    data = s.run(script)
    
    # 将DolphinDB数据转换为vn.py的BarData列表
    history_data = []
    for index, row in data.iterrows():
        bar = BarData(
            symbol=row['symbol'],
            exchange=None, # 根据需要填入
            datetime=row['datetime'],
            interval=Interval.MINUTE,
            open_price=row['open'],
            high_price=row['high'],
            low_price=row['low'],
            close_price=row['close'],
            volume=row['volume'],
            gateway_name="DB"
        )
        history_data.append(bar)

    # 3. 配置引擎
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=f"{target_symbol}.LOCAL",
        interval=Interval.MINUTE,
        start=start_date,
        end=end_date,
        rate=0.0001,   # 手续费万一
        slippage=0.2,  # 滑点
        size=10,       # 合约乘数
        pricetick=1,   # 价格跳动
        capital=100000
    )
    
    engine.add_strategy(ModularDoubleMaStrategy, {})
    engine.history_data = history_data
    
    # 4. 运行回测
    engine.run_backtesting()
    df_results = engine.calculate_result()
    statistics = engine.calculate_statistics()
    
    # 5. 计算 Van Tharp 绩效指标
    r_list = engine.strategy.r_multiples
    if r_list:
        win_rate = len([r for r in r_list if r > 0]) / len(r_list)
        avg_r = np.mean(r_list)
        std_r = np.std(r_list)
        sqn = (len(r_list)**0.5) * (avg_r / std_r) if std_r != 0 else 0
        
        print("\n" + "="*30)
        print("--- Van Tharp 核心绩效 ---")
        print(f"总交易笔数: {len(r_list)}")
        print(f"胜率: {win_rate:.2%}")
        print(f"平均R倍数: {avg_r:.2f}R")
        print(f"R倍数标准差: {std_r:.2f}")
        print(f"SQN评分: {sqn:.2f}")
        print(f"总R收益: {sum(r_list):.2f}R")
        print("="*30)
    
    return statistics

# 运行
if __name__ == "__main__":
    stats = run_backtest("AG2005")