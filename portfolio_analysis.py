import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import matplotlib as mpl
import numpy as np

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def calculate_max_drawdown(returns):
    """计算最大回撤及其发生日期"""
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    max_drawdown = drawdown.min()
    max_drawdown_date = drawdown.idxmin()
    
    # 找到最大回撤开始的时间（即最高点）
    peak_date = cumulative[:max_drawdown_date].idxmax()
    
    return max_drawdown, max_drawdown_date, peak_date

def load_data():
    # 读取收益率数据
    returns_path = r'C:\Users\Admin\Desktop\BTC每日收益率矩阵.xlsx'
    allocation_path = r'C:\Users\Admin\Desktop\仓位分配.xlsx'
    
    # 读取收益率数据
    returns_df = pd.read_excel(returns_path)
    # 读取仓位分配数据
    allocation_df = pd.read_excel(allocation_path)
    return returns_df, allocation_df

def calculate_portfolio_returns(returns_df, allocation_df):
    # 确保日期列对齐
    returns_df.set_index(returns_df.columns[0], inplace=True)
    
    # 计算每个策略的累积收益
    portfolio_returns = {}
    for column in allocation_df.columns[1:]:  # 跳过第一列（日期列）
        weights = allocation_df[column].values
        strategy_returns = (returns_df * weights).sum(axis=1)
        cumulative_returns = (1 + strategy_returns).cumprod() - 1
        portfolio_returns[column] = cumulative_returns
    
    return pd.DataFrame(portfolio_returns)

def plot_returns(portfolio_returns):
    plt.figure(figsize=(15, 8))
    
    # 计算并存储每个策略的最大回撤
    max_drawdowns = {}
    for column in portfolio_returns.columns:
        returns = portfolio_returns[column]
        max_drawdown, max_drawdown_date, peak_date = calculate_max_drawdown(returns)
        max_drawdowns[column] = (max_drawdown, max_drawdown_date, peak_date)
        
        # 绘制收益曲线
        plt.plot(returns.index, returns, label=f'{column}')
        
        # 在最大回撤点添加标注
        plt.annotate(f'最大回撤: {max_drawdown:.2%}\n开始: {peak_date.strftime("%Y-%m-%d")}\n结束: {max_drawdown_date.strftime("%Y-%m-%d")}',
                    xy=(max_drawdown_date, returns[max_drawdown_date]),
                    xytext=(10, 10),
                    textcoords='offset points',
                    bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    plt.title('不同仓位分配策略的累积收益对比')
    plt.xlabel('日期')
    plt.ylabel('累积收益率')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # 在图表下方添加最大回撤汇总表格
    table_data = [[strategy, 
                  f'{drawdown[0]:.2%}', 
                  drawdown[1].strftime('%Y-%m-%d'),
                  drawdown[2].strftime('%Y-%m-%d')] 
                 for strategy, drawdown in max_drawdowns.items()]
    
    table = plt.table(cellText=table_data,
                     colLabels=['策略', '最大回撤', '回撤结束日期', '回撤开始日期'],
                     loc='bottom',
                     bbox=[0.0, -0.4, 1.0, 0.3])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)
    
    plt.subplots_adjust(bottom=0.4)  # 为表格留出空间
    plt.savefig('portfolio_returns_comparison.png', bbox_inches='tight', dpi=300)
    plt.close()
    
    # 将回撤信息保存到Excel
    drawdown_df = pd.DataFrame(table_data, columns=['策略', '最大回撤', '回撤结束日期', '回撤开始日期'])
    drawdown_df.to_excel('max_drawdown_analysis.xlsx', index=False)

def main():
    try:
        # 加载数据
        returns_df, allocation_df = load_data()
        
        # 计算投资组合收益
        portfolio_returns = calculate_portfolio_returns(returns_df, allocation_df)
        
        # 绘制收益对比图
        plot_returns(portfolio_returns)
        
        # 保存结果到Excel
        portfolio_returns.to_excel('portfolio_returns_results.xlsx')
        
        print("分析完成！")
        print("1. 收益结果已保存到 'portfolio_returns_results.xlsx'")
        print("2. 图表已保存到 'portfolio_returns_comparison.png'")
        print("3. 回撤分析已保存到 'max_drawdown_analysis.xlsx'")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")

if __name__ == "__main__":
    main() 