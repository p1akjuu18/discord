import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import FontProperties
import numpy as np

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def match_trades(df):
    # 按交易对分组
    trades_by_symbol = {}
    for symbol in df['Symbol'].unique():
        symbol_trades = df[df['Symbol'] == symbol].copy()
        
        # 分别获取买入和卖出订单
        buys = symbol_trades[symbol_trades['Side'] == 'BUY'].copy()
        sells = symbol_trades[symbol_trades['Side'] == 'SELL'].copy()
        
        # 按时间排序
        buys = buys.sort_values('Time(UTC)')
        sells = sells.sort_values('Time(UTC)')
        
        trades = []
        remaining_buys = buys.copy()
        remaining_sells = sells.copy()
        
        # 匹配交易
        for _, buy in buys.iterrows():
            buy_amount = buy['Executed Amount']
            buy_price = buy['Average Price']
            buy_time = buy['Time(UTC)']
            buy_quote_amount = buy['Executed Quote Amount']
            
            # 找到对应的卖出订单
            matching_sells = remaining_sells[remaining_sells['Time(UTC)'] > buy_time]
            
            for _, sell in matching_sells.iterrows():
                sell_amount = sell['Executed Amount']
                sell_price = sell['Average Price']
                sell_time = sell['Time(UTC)']
                sell_quote_amount = sell['Executed Quote Amount']
                
                # 计算匹配数量
                matched_amount = min(buy_amount, sell_amount)
                
                if matched_amount > 0:
                    # 计算这笔交易的盈亏
                    profit = (sell_price - buy_price) * matched_amount
                    profit_percentage = (sell_price - buy_price) / buy_price * 100
                    
                    # 计算匹配的交易额
                    matched_quote_amount = (buy_quote_amount + sell_quote_amount) / 2
                    
                    trades.append({
                        'Symbol': symbol,
                        'Entry Time': buy_time,
                        'Exit Time': sell_time,
                        'Entry Price': buy_price,
                        'Exit Price': sell_price,
                        'Amount': matched_amount,
                        'Entry Quote Amount': buy_quote_amount,
                        'Exit Quote Amount': sell_quote_amount,
                        'Executed Quote Amount': matched_quote_amount,
                        'Profit': profit,
                        'Profit Percentage': profit_percentage,
                        'Holding Time': (pd.to_datetime(sell_time) - pd.to_datetime(buy_time)).total_seconds() / 3600  # 持仓时间（小时）
                    })
                    
                    # 更新剩余数量
                    buy_amount -= matched_amount
                    sell_amount -= matched_amount
                    
                    # 更新卖出订单的剩余数量
                    remaining_sells.loc[sell.name, 'Executed Amount'] = sell_amount
                    
                    if buy_amount == 0:
                        break
            
            # 更新买入订单的剩余数量
            remaining_buys.loc[buy.name, 'Executed Amount'] = buy_amount
        
        trades_by_symbol[symbol] = pd.DataFrame(trades)
    
    return trades_by_symbol

def create_visualizations(trades_by_symbol, output_dir):
    """创建可视化图表"""
    # 设置全局样式
    plt.style.use('bmh')  # 使用matplotlib内置的bmh样式
    colors = ['#2ecc71', '#e74c3c', '#3498db', '#f1c40f', '#9b59b6', '#1abc9c']
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    # 创建图表目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 创建Excel写入器
    daily_stats_output = os.path.join(output_dir, "币种每日收益率分析.xlsx")
    with pd.ExcelWriter(daily_stats_output, engine='openpyxl') as writer:
        # 为每个币种创建每日分析
        for symbol, trades in trades_by_symbol.items():
            if len(trades) > 0:
                # 确保日期格式正确
                trades['Date'] = pd.to_datetime(trades['Entry Time']).dt.date
                
                # 计算每日详细统计数据
                daily_stats = trades.groupby('Date', observed=True).agg({
                    'Profit': ['sum', 'mean', 'count'],
                    'Executed Quote Amount': 'sum',
                    'Profit Percentage': ['mean', 'max', 'min']
                }).round(4)
                
                # 重命名列
                daily_stats.columns = [
                    '总收益', '平均收益', '交易次数',
                    '交易总额', '平均收益率', '最大收益率', '最小收益率'
                ]
                
                # 计算每日投入本金和收益率
                daily_stats['投入本金'] = daily_stats['交易总额'] / 20
                daily_stats['日收益率'] = (daily_stats['总收益'] / daily_stats['投入本金'] * 100).round(2)
                
                # 计算累计收益率
                daily_stats['累计收益'] = daily_stats['总收益'].cumsum()
                daily_stats['累计投入本金'] = daily_stats['投入本金'].cumsum()
                daily_stats['累计收益率'] = (daily_stats['累计收益'] / daily_stats['累计投入本金'] * 100).round(2)
                
                # 计算每日胜率
                daily_win_rates = []
                for date in daily_stats.index:
                    day_trades = trades[trades['Date'] == date]
                    win_rate = len(day_trades[day_trades['Profit'] > 0]) / len(day_trades) * 100
                    daily_win_rates.append(win_rate)
                
                daily_stats['胜率'] = daily_win_rates
                
                # 添加总体统计
                total_stats = pd.DataFrame({
                    '指标': [
                        '总交易次数',
                        '盈利交易次数',
                        '亏损交易次数',
                        '总胜率',
                        '总收益',
                        '平均收益',
                        '最大收益',
                        '最小收益',
                        '平均收益率',
                        '最大收益率',
                        '最小收益率',
                        '总投入本金',
                        '总收益率'
                    ],
                    '数值': [
                        len(trades),
                        len(trades[trades['Profit'] > 0]),
                        len(trades[trades['Profit'] <= 0]),
                        f"{len(trades[trades['Profit'] > 0]) / len(trades) * 100:.2f}%",
                        f"{trades['Profit'].sum():.4f}",
                        f"{trades['Profit'].mean():.4f}",
                        f"{trades['Profit'].max():.4f}",
                        f"{trades['Profit'].min():.4f}",
                        f"{trades['Profit Percentage'].mean():.2f}%",
                        f"{trades['Profit Percentage'].max():.2f}%",
                        f"{trades['Profit Percentage'].min():.2f}%",
                        f"{trades['Executed Quote Amount'].sum() / 20:.4f}",
                        f"{(trades['Profit'].sum() / (trades['Executed Quote Amount'].sum() / 20) * 100):.2f}%"
                    ]
                })
                
                # 将数据写入Excel的不同sheet
                daily_stats.to_excel(writer, sheet_name=f'{symbol}_每日统计')
                total_stats.to_excel(writer, sheet_name=f'{symbol}_总体统计', index=False)
                
                # 创建该币种的每日收益率分析图
                plt.figure(figsize=(15, 10))
                
                # 创建子图
                fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
                
                # 1. 每日收益率柱状图
                bars1 = ax1.bar(daily_stats.index, daily_stats['日收益率'], 
                               color=[colors[0] if x >= 0 else colors[1] for x in daily_stats['日收益率']])
                ax1.set_title('每日收益率', fontsize=12)
                ax1.set_ylabel('收益率(%)', fontsize=10)
                ax1.tick_params(axis='x', rotation=45)
                
                # 添加数值标签
                for bar in bars1:
                    height = bar.get_height()
                    ax1.text(bar.get_x() + bar.get_width()/2., height,
                            f'{height:.1f}%',
                            ha='center', va='bottom' if height >= 0 else 'top')
                
                # 2. 累计收益率曲线
                ax2.plot(daily_stats.index, daily_stats['累计收益率'], 
                        color=colors[2], linewidth=2, marker='o')
                ax2.set_title('累计收益率走势', fontsize=12)
                ax2.set_ylabel('累计收益率(%)', fontsize=10)
                ax2.tick_params(axis='x', rotation=45)
                ax2.grid(True, alpha=0.3)
                
                # 3. 每日交易次数和胜率
                ax3_twin = ax3.twinx()
                bars3 = ax3.bar(daily_stats.index, daily_stats['交易次数'], 
                               color=colors[3], alpha=0.7, label='交易次数')
                ax3.set_ylabel('交易次数', color=colors[3], fontsize=10)
                ax3.tick_params(axis='y', labelcolor=colors[3])
                
                line3 = ax3_twin.plot(daily_stats.index, daily_stats['胜率'], 
                                     color=colors[4], linewidth=2, marker='o', label='胜率')
                ax3_twin.set_ylabel('胜率(%)', color=colors[4], fontsize=10)
                ax3_twin.tick_params(axis='y', labelcolor=colors[4])
                
                ax3.set_title('每日交易次数和胜率', fontsize=12)
                ax3.tick_params(axis='x', rotation=45)
                
                # 合并图例
                lines3, labels3 = ax3.get_legend_handles_labels()
                lines3_twin, labels3_twin = ax3_twin.get_legend_handles_labels()
                ax3.legend(lines3 + lines3_twin, labels3 + labels3_twin, loc='upper left')
                
                # 4. 每日收益分布
                ax4.hist(daily_stats['日收益率'], bins=20, color=colors[5], alpha=0.7)
                ax4.set_title('日收益率分布', fontsize=12)
                ax4.set_xlabel('收益率(%)', fontsize=10)
                ax4.set_ylabel('频次', fontsize=10)
                ax4.grid(True, alpha=0.3)
                
                plt.suptitle(f'{symbol} 每日交易分析总览', fontsize=14, y=1.02)
                plt.tight_layout()
                plt.savefig(os.path.join(output_dir, f'{symbol}_每日交易分析图.png'), dpi=300, bbox_inches='tight')
                plt.close()
    
    # 合并所有交易数据
    all_trades = pd.concat([trades for trades in trades_by_symbol.values() if len(trades) > 0])
    all_trades['Date'] = pd.to_datetime(all_trades['Entry Time']).dt.date
    all_trades['Entry Time'] = pd.to_datetime(all_trades['Entry Time'])
    
    # 添加新的时间分析
    all_trades['Hour'] = pd.to_datetime(all_trades['Entry Time']).dt.hour
    all_trades['Time Period'] = pd.cut(all_trades['Hour'], 
                                     bins=[0, 6, 12, 18, 24],
                                     labels=['凌晨(0-6点)', '上午(6-12点)', '下午(12-18点)', '晚上(18-24点)'])
    
    # 计算每日交易额和收益率
    daily_stats = all_trades.groupby('Date').agg({
        'Executed Quote Amount': 'sum',
        'Profit': 'sum'
    }).reset_index()
    
    # 计算每日投入本金（交易额/20）和收益率
    daily_stats['Daily Capital'] = daily_stats['Executed Quote Amount'] / 20
    daily_stats['Daily Return Rate'] = (daily_stats['Profit'] / daily_stats['Daily Capital'] * 100)
    
    # 计算累计收益率
    daily_stats['Cumulative Return Rate'] = daily_stats['Daily Return Rate'].cumsum()
    
    # 1. 交易盈亏分析图 - 优化版
    plt.figure(figsize=(15, 8))
    cumulative_profit = all_trades['Profit'].cumsum()
    
    # 创建主坐标轴
    ax1 = plt.gca()
    
    # 绘制累计盈利曲线
    ax1.plot(all_trades['Entry Time'], cumulative_profit, color=colors[0], linewidth=2, label='累计盈利')
    ax1.set_ylabel('累计盈利', color=colors[0], fontsize=12)
    ax1.tick_params(axis='y', labelcolor=colors[0])
    
    # 创建第二个y轴
    ax2 = ax1.twinx()
    
    # 绘制单笔交易盈亏条形图
    profit_mask = all_trades['Profit'] > 0
    loss_mask = ~profit_mask
    
    # 绘制盈利条形
    if profit_mask.any():
        ax2.bar(all_trades.loc[profit_mask, 'Entry Time'], 
                all_trades.loc[profit_mask, 'Profit'],
                color=colors[0], alpha=0.7, label='盈利交易', width=0.8)
    
    # 绘制亏损条形
    if loss_mask.any():
        ax2.bar(all_trades.loc[loss_mask, 'Entry Time'], 
                all_trades.loc[loss_mask, 'Profit'],
                color=colors[1], alpha=0.7, label='亏损交易', width=0.8)
    
    ax2.set_ylabel('单笔收益', color=colors[1], fontsize=12)
    ax2.tick_params(axis='y', labelcolor=colors[1])
    
    # 设置x轴格式
    plt.gcf().autofmt_xdate()
    ax1.set_xlabel('日期', fontsize=12)
    
    # 添加图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    # 添加网格
    ax1.grid(True, alpha=0.3)
    
    # 添加标题和统计信息
    total_profit = all_trades['Profit'].sum()
    win_rate = len(all_trades[all_trades['Profit'] > 0]) / len(all_trades) * 100
    plt.title(f'交易盈亏分析图\n总盈利: {total_profit:.2f} | 胜率: {win_rate:.1f}%', fontsize=14)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '交易盈亏分析图.png'), dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. 每日收益率分析图 - 优化版
    plt.figure(figsize=(15, 8))
    
    # 创建主坐标轴
    ax1 = plt.gca()
    
    # 绘制每日收益率
    bars = ax1.bar(daily_stats['Date'], daily_stats['Daily Return Rate'], 
                   color=colors[2], alpha=0.7, label='日收益率')
    ax1.set_ylabel('日收益率(%)', color=colors[2], fontsize=12)
    ax1.tick_params(axis='y', labelcolor=colors[2])
    
    # 添加数值标签
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}%',
                ha='center', va='bottom' if height >= 0 else 'top')
    
    # 创建第二个y轴
    ax2 = ax1.twinx()
    
    # 绘制累计收益率
    ax2.plot(daily_stats['Date'], daily_stats['Cumulative Return Rate'], 
             color=colors[3], linewidth=2, label='累计收益率')
    ax2.set_ylabel('累计收益率(%)', color=colors[3], fontsize=12)
    ax2.tick_params(axis='y', labelcolor=colors[3])
    
    # 设置x轴格式
    plt.gcf().autofmt_xdate()
    ax1.set_xlabel('日期', fontsize=12)
    
    # 添加图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    # 添加网格
    ax1.grid(True, alpha=0.3)
    
    # 添加标题和统计信息
    avg_daily_return = daily_stats['Daily Return Rate'].mean()
    max_daily_return = daily_stats['Daily Return Rate'].max()
    plt.title(f'每日收益率分析图\n平均日收益率: {avg_daily_return:.1f}% | 最大日收益率: {max_daily_return:.1f}%', fontsize=14)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '每日收益率分析图.png'), dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. 日线级别盈亏图
    plt.figure(figsize=(15, 8))
    daily_profit = all_trades.groupby('Date')['Profit'].sum()
    daily_profit = daily_profit.sort_index()
    
    # 创建柱状图，盈利为绿色，亏损为红色
    colors = ['g' if x > 0 else 'r' for x in daily_profit]
    plt.bar(range(len(daily_profit)), daily_profit.values, color=colors)
    plt.title('日线级别盈亏图')
    plt.xlabel('日期')
    plt.ylabel('日盈亏')
    plt.xticks(range(len(daily_profit)), [str(date) for date in daily_profit.index], rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '日线级别盈亏图.png'))
    plt.close()
    
    # 4. 累计盈利曲线
    plt.figure(figsize=(15, 8))
    cumulative_daily_profit = daily_profit.cumsum()
    plt.plot(range(len(cumulative_daily_profit)), cumulative_daily_profit.values, 'b-', linewidth=2)
    plt.title('累计盈利曲线')
    plt.xlabel('日期')
    plt.ylabel('累计盈利')
    plt.xticks(range(len(cumulative_daily_profit)), [str(date) for date in cumulative_daily_profit.index], rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '累计盈利曲线.png'))
    plt.close()
    
    # 5. 每日总收益折线图
    plt.figure(figsize=(15, 8))
    
    # 按日期汇总所有交易对的收益
    daily_total_profit = all_trades.groupby('Date')['Profit'].sum()
    
    # 计算累计收益
    cumulative_profit = daily_total_profit.cumsum()
    
    # 创建子图
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
    
    # 绘制每日收益柱状图
    ax1.bar(daily_total_profit.index, daily_total_profit.values, color='skyblue')
    ax1.set_title('每日收益柱状图')
    ax1.set_xlabel('日期')
    ax1.set_ylabel('每日收益')
    ax1.tick_params(axis='x', rotation=45)
    
    # 绘制累计收益曲线图
    ax2.plot(cumulative_profit.index, cumulative_profit.values, color='red', marker='o')
    ax2.set_title('累计收益曲线图')
    ax2.set_xlabel('日期')
    ax2.set_ylabel('累计收益')
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '每日收益走势图.png'))
    plt.close()
    
    # 6. 各交易对胜率对比柱状图
    plt.figure(figsize=(12, 6))
    win_rates = []
    symbols = []
    for symbol, trades in trades_by_symbol.items():
        if len(trades) > 0:
            win_rate = len(trades[trades['Profit'] > 0]) / len(trades) * 100
            win_rates.append(win_rate)
            symbols.append(symbol)
    
    plt.bar(symbols, win_rates)
    plt.title('各交易对胜率对比')
    plt.xlabel('交易对')
    plt.ylabel('胜率(%)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '胜率对比图.png'))
    plt.close()
    
    # 7. 各交易对总盈亏对比柱状图
    plt.figure(figsize=(12, 6))
    total_profits = []
    for symbol, trades in trades_by_symbol.items():
        if len(trades) > 0:
            total_profits.append(trades['Profit'].sum())
    
    plt.bar(symbols, total_profits)
    plt.title('各交易对总盈亏对比')
    plt.xlabel('交易对')
    plt.ylabel('总盈亏')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '总盈亏对比图.png'))
    plt.close()
    
    # 8. 交易量分布饼图
    plt.figure(figsize=(10, 10))
    volumes = []
    for symbol, trades in trades_by_symbol.items():
        if len(trades) > 0:
            volumes.append(trades['Amount'].sum())
    
    plt.pie(volumes, labels=symbols, autopct='%1.1f%%')
    plt.title('交易量分布')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '交易量分布图.png'))
    plt.close()
    
    # 9. 盈亏分布直方图
    plt.figure(figsize=(12, 6))
    all_profits = []
    for trades in trades_by_symbol.values():
        if len(trades) > 0:
            all_profits.extend(trades['Profit'].values)
    
    plt.hist(all_profits, bins=50)
    plt.title('盈亏分布直方图')
    plt.xlabel('盈亏')
    plt.ylabel('频次')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '盈亏分布图.png'))
    plt.close()
    
    # 10. 持仓时间与收益的散点图
    plt.figure(figsize=(12, 6))
    for symbol, trades in trades_by_symbol.items():
        if len(trades) > 0:
            plt.scatter(trades['Holding Time'], trades['Profit'], label=symbol, alpha=0.5)
    
    plt.title('持仓时间与收益关系图')
    plt.xlabel('持仓时间(小时)')
    plt.ylabel('收益')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '持仓时间收益关系图.png'))
    plt.close()
    
    # 11. 盈亏比分析图 - 优化版
    plt.figure(figsize=(15, 10))
    
    # 创建子图
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. 平均盈亏对比
    profit_trades = all_trades[all_trades['Profit'] > 0]
    loss_trades = all_trades[all_trades['Profit'] <= 0]
    
    avg_profit = profit_trades['Profit'].mean() if not profit_trades.empty else 0
    avg_loss = loss_trades['Profit'].mean() if not loss_trades.empty else 0
    
    bars1 = ax1.bar(['平均盈利', '平均亏损'], [avg_profit, abs(avg_loss)], color=[colors[0], colors[1]])
    ax1.set_title('平均盈亏对比', fontsize=12)
    ax1.set_ylabel('金额', fontsize=10)
    
    # 添加数值标签
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom')
    
    # 2. 最大盈亏对比
    max_profit = all_trades['Profit'].max()
    max_loss = all_trades['Profit'].min()
    
    bars2 = ax2.bar(['最大盈利', '最大亏损'], [max_profit, abs(max_loss)], color=[colors[0], colors[1]])
    ax2.set_title('最大盈亏对比', fontsize=12)
    ax2.set_ylabel('金额', fontsize=10)
    
    # 添加数值标签
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom')
    
    # 3. 盈亏交易数量对比
    profit_count = len(profit_trades)
    loss_count = len(loss_trades)
    total_count = profit_count + loss_count
    
    ax3.pie([profit_count, loss_count], labels=['盈利交易', '亏损交易'], 
            autopct=lambda p: f'{p:.1f}%\n({int(p*total_count/100)})',
            colors=[colors[0], colors[1]])
    ax3.set_title('盈亏交易数量分布', fontsize=12)
    
    # 4. 盈亏金额分布
    profit_amount = profit_trades['Profit'].sum()
    loss_amount = loss_trades['Profit'].sum()
    total_amount = profit_amount + abs(loss_amount)
    
    ax4.pie([profit_amount, abs(loss_amount)], labels=['盈利总额', '亏损总额'], 
            autopct=lambda p: f'{p:.1f}%\n({p*total_amount/100:.2f})',
            colors=[colors[0], colors[1]])
    ax4.set_title('盈亏金额分布', fontsize=12)
    
    plt.suptitle('交易盈亏分析总览', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '盈亏比分析图.png'), dpi=300, bbox_inches='tight')
    plt.close()
    
    # 12. 时间维度分析图 - 优化版
    plt.figure(figsize=(15, 10))
    
    # 创建子图
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    
    # 1. 不同时间段的表现
    time_period_stats = all_trades.groupby('Time Period').agg({
        'Profit': ['count', 'sum', 'mean'],
        'Profit Percentage': 'mean'
    }).round(2)
    
    # 绘制不同时间段的交易数量
    bars1 = time_period_stats[('Profit', 'count')].plot(kind='bar', ax=ax1, color=colors[2])
    ax1.set_title('不同时间段的交易数量', fontsize=12)
    ax1.set_ylabel('交易数量', fontsize=10)
    ax1.set_xlabel('时间段', fontsize=10)
    
    # 添加数值标签
    for bar in bars1.patches:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom')
    
    # 2. 不同时间段的平均收益率
    bars2 = time_period_stats[('Profit Percentage', 'mean')].plot(kind='bar', ax=ax2, color=colors[3])
    ax2.set_title('不同时间段的平均收益率', fontsize=12)
    ax2.set_ylabel('平均收益率(%)', fontsize=10)
    ax2.set_xlabel('时间段', fontsize=10)
    
    # 添加数值标签
    for bar in bars2.patches:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}%',
                ha='center', va='bottom' if height >= 0 else 'top')
    
    plt.suptitle('交易时间分析', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '时间维度分析图.png'), dpi=300, bbox_inches='tight')
    plt.close()

def analyze_trade_history(file_path):
    try:
        # 读取CSV文件，尝试不同的编码方式
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='gbk')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='gb2312')
        
        # 匹配交易
        trades_by_symbol = match_trades(df)
        
        # 创建Excel写入器
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
        output_file = os.path.join(desktop_path, "交易分析报告.xlsx")
        
        # 创建图表
        charts_dir = os.path.join(desktop_path, "交易分析图表")
        create_visualizations(trades_by_symbol, charts_dir)
        
        # 生成每日收益率总结表
        all_trades = pd.concat([trades for trades in trades_by_symbol.values() if len(trades) > 0])
        all_trades['Date'] = pd.to_datetime(all_trades['Entry Time']).dt.date
        
        # 确保使用history文件中的Executed Quote Amount
        daily_stats = all_trades.groupby('Date').agg({
            'Executed Quote Amount': 'sum',
            'Profit': 'sum'
        }).reset_index()
        
        # 计算每日投入本金（使用history文件中的Executed Quote Amount）
        daily_stats['每日投入本金'] = daily_stats['Executed Quote Amount'] / 20
        daily_stats['每日收益率(%)'] = daily_stats['Profit'] / daily_stats['每日投入本金'] * 100
        
        # 计算累计收益和累计投入本金
        daily_stats['累计收益'] = daily_stats['Profit'].cumsum()
        daily_stats['累计投入本金'] = daily_stats['每日投入本金'].cumsum()
        
        # 计算累计收益率
        daily_stats['累计收益率(%)'] = daily_stats['累计收益'] / daily_stats['累计投入本金'] * 100
        
        # 重命名列
        daily_stats.rename(columns={
            'Date': '日期',
            'Executed Quote Amount': '每日交易总额',
            'Profit': '每日盈亏'
        }, inplace=True)

        # 单独导出每日收益率总结表excel
        daily_stats_output = os.path.join(desktop_path, "每日收益率分析.xlsx")
        daily_stats.to_excel(daily_stats_output, index=False)

        # 继续写入主报告
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 写入每日收益率总结表（主报告中保留，方便查阅）
            daily_stats.to_excel(writer, sheet_name='每日收益率总结表', index=False)
            # 创建统计DataFrame
            stats_data = []
            total_trades = 0
            total_profit = 0
            winning_trades = 0
            total_holding_time = 0
            all_trades_list = []
            
            # 处理每个交易对的数据
            for symbol, trades in trades_by_symbol.items():
                if len(trades) > 0:
                    all_trades_list.append(trades)
                    # 计算统计数据
                    stats = {
                        '交易对': symbol,
                        '总交易次数': len(trades),
                        '盈利交易次数': len(trades[trades['Profit'] > 0]),
                        '亏损交易次数': len(trades[trades['Profit'] <= 0]),
                        '胜率': f"{(len(trades[trades['Profit'] > 0]) / len(trades) * 100):.2f}%",
                        '总盈亏': f"{trades['Profit'].sum():.8f}",
                        '平均盈亏': f"{trades['Profit'].mean():.8f}",
                        '平均收益率': f"{trades['Profit Percentage'].mean():.2f}%",
                        '整体收益率': f"{(trades['Profit'].sum() / (trades['Entry Quote Amount'] + trades['Exit Quote Amount']).sum() * 100):.2f}%",
                        '最大盈利': f"{trades['Profit'].max():.8f}",
                        '最大亏损': f"{trades['Profit'].min():.8f}",
                        '平均持仓时间(小时)': f"{trades['Holding Time'].mean():.2f}",
                        '总交易额': f"{(trades['Entry Quote Amount'] + trades['Exit Quote Amount']).sum():.8f}",
                        '总交易次数': f"{len(trades)}"
                    }
                    stats_data.append(stats)
                    
                    # 将详细交易数据写入单独的sheet
                    trades.to_excel(writer, sheet_name=f'{symbol}_详细交易', index=False)
                    
                    total_trades += len(trades)
                    total_profit += trades['Profit'].sum()
                    winning_trades += len(trades[trades['Profit'] > 0])
                    total_holding_time += trades['Holding Time'].sum()
            
            # 添加总体统计
            if total_trades > 0:
                all_trades = pd.concat(all_trades_list) if all_trades_list else pd.DataFrame()
                stats_data.append({
                    '交易对': '总体统计',
                    '总交易次数': total_trades,
                    '盈利交易次数': winning_trades,
                    '亏损交易次数': total_trades - winning_trades,
                    '胜率': f"{(winning_trades / total_trades * 100):.2f}%",
                    '总盈亏': f"{total_profit:.8f}",
                    '平均盈亏': f"{(total_profit / total_trades):.8f}",
                    '平均收益率': f"{(all_trades['Profit Percentage'].mean()):.2f}%" if not all_trades.empty else "N/A",
                    '整体收益率': f"{(total_profit / (all_trades['Entry Quote Amount'] + all_trades['Exit Quote Amount']).sum() * 100):.2f}%" if not all_trades.empty else "N/A",
                    '最大盈利': 'N/A',
                    '最大亏损': 'N/A',
                    '平均持仓时间(小时)': f"{(total_holding_time / total_trades):.2f}",
                    '总交易额': f"{(all_trades['Entry Quote Amount'] + all_trades['Exit Quote Amount']).sum():.8f}" if not all_trades.empty else "N/A",
                    '总交易次数': f"{total_trades}"
                })
            
            # 将统计表写入Excel
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='交易统计', index=False)
            
            # 设置列宽
            worksheet = writer.sheets['交易统计']
            for idx, col in enumerate(stats_df.columns):
                max_length = max(
                    stats_df[col].astype(str).apply(len).max(),
                    len(str(col))
                )
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)
        
        print(f"\n分析报告已保存到: {output_file}")
        print(f"分析图表已保存到: {charts_dir}")
        
    except Exception as e:
        print(f"分析过程中出现错误: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    # 获取桌面路径
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    history_file = os.path.join(desktop_path, "history.csv")
    
    if os.path.exists(history_file):
        analyze_trade_history(history_file)
    else:
        print(f"未找到交易历史文件: {history_file}") 