import pandas as pd
import numpy as np

def analyze_trading_data():
    # 读取Excel文件
    print("正在读取交易数据...")
    try:
        df = pd.read_excel('result.xlsx')
        
        # 确保数值类型正确 - 将总计加权收益pct列转换为数值型
        df['总计加权收益pct'] = pd.to_numeric(df['总计加权收益pct'], errors='coerce')
        
        # 按频道分组
        grouped = df.groupby('频道')
        
        # 创建结果DataFrame
        results = pd.DataFrame(columns=[
            '频道', '总交易数', '盈利交易数', '亏损交易数', 
            '平均收益(%)', '最大收益(%)', '最小收益(%)', '胜率(%)'
        ])
        
        # 计算每个频道的统计数据
        for channel, group in grouped:
            # 过滤掉无法识别结果的行
            valid_trades = group.dropna(subset=['总计加权收益pct'])
            
            # 总交易数
            total_trades = len(valid_trades)
            
            if total_trades > 0:
                # 盈利和亏损交易
                profit_trades = sum(valid_trades['总计加权收益pct'] > 0)
                loss_trades = sum(valid_trades['总计加权收益pct'] <= 0)
                
                # 平均收益
                avg_profit = valid_trades['总计加权收益pct'].mean()
                
                # 最大和最小收益
                max_profit = valid_trades['总计加权收益pct'].max()
                min_profit = valid_trades['总计加权收益pct'].min()
                
                # 胜率
                win_rate = (profit_trades / total_trades) * 100 if total_trades > 0 else 0
                
                # 添加到结果DataFrame
                results = pd.concat([results, pd.DataFrame({
                    '频道': [channel],
                    '总交易数': [total_trades],
                    '盈利交易数': [profit_trades],
                    '亏损交易数': [loss_trades],
                    '平均收益(%)': [round(avg_profit, 2)],
                    '最大收益(%)': [round(max_profit, 2)],
                    '最小收益(%)': [round(min_profit, 2)],
                    '胜率(%)': [round(win_rate, 2)]
                })], ignore_index=True)
        
        # 添加总计行
        all_valid_trades = df.dropna(subset=['总计加权收益pct'])
        if len(all_valid_trades) > 0:
            total_all = len(all_valid_trades)
            profit_all = sum(all_valid_trades['总计加权收益pct'] > 0)
            loss_all = sum(all_valid_trades['总计加权收益pct'] <= 0)
            avg_profit_all = all_valid_trades['总计加权收益pct'].mean()
            max_profit_all = all_valid_trades['总计加权收益pct'].max()
            min_profit_all = all_valid_trades['总计加权收益pct'].min()
            win_rate_all = (profit_all / total_all) * 100 if total_all > 0 else 0
            
            results = pd.concat([results, pd.DataFrame({
                '频道': ['总计'],
                '总交易数': [total_all],
                '盈利交易数': [profit_all],
                '亏损交易数': [loss_all],
                '平均收益(%)': [round(avg_profit_all, 2)],
                '最大收益(%)': [round(max_profit_all, 2)],
                '最小收益(%)': [round(min_profit_all, 2)],
                '胜率(%)': [round(win_rate_all, 2)]
            })], ignore_index=True)
        
        # 按总交易数降序排序
        results = results.sort_values(by='总交易数', ascending=False)
        
        # 打印结果表格
        print("\n各频道交易统计数据:")
        print(results.to_string(index=False))
        
        # 保存结果到Excel文件
        results.to_excel('交易统计结果.xlsx', index=False)
        print("\n结果已保存到'交易统计结果.xlsx'文件")
        
    except Exception as e:
        print(f"处理数据时出错: {e}")

if __name__ == "__main__":
    analyze_trading_data() 