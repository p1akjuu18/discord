import pandas as pd
import os
from datetime import datetime

def analyze_trading_symbols(file_path):
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return
    
    try:
        # 读取Excel文件
        print(f"正在读取文件: {file_path}")
        df = pd.read_excel(file_path)
        
        # 显示所有列名，帮助识别交易币种列
        print("文件中的列名:")
        for i, col in enumerate(df.columns):
            print(f"{i}: {col}")
        
        # 请用户选择包含交易币种的列
        col_index = int(input("请输入包含交易币种的列索引号: "))
        symbol_col = df.columns[col_index]
        
        # 提取不同的交易币种
        symbols = df[symbol_col].dropna().str.strip()
        
        # 有些币种可能包含在格式如"BTC/USDT"的对中，提取主币种
        def extract_base_symbol(symbol):
            if isinstance(symbol, str):
                # 处理常见的分隔符
                for sep in ['/', '-', '_']:
                    if sep in symbol:
                        parts = symbol.split(sep)
                        # 通常第一部分是基础币种
                        return parts[0].strip()
                
                # 处理带有后缀的情况，如BTCUSDT
                for suffix in ['USDT', 'USD', 'BTC', 'ETH']:
                    if symbol.endswith(suffix):
                        return symbol[:-len(suffix)]
            
            # 如果无法解析，返回原始值
            return symbol
        
        # 提取基础币种
        base_symbols = symbols.apply(extract_base_symbol)
        
        # 统计不同币种数量
        unique_symbols = base_symbols.unique()
        
        print(f"\n在文件中找到 {len(unique_symbols)} 个不同的交易币种:")
        for i, symbol in enumerate(sorted(unique_symbols), 1):
            print(f"{i}. {symbol}")
        
        # 创建结果DataFrame
        result_df = pd.DataFrame({
            '序号': range(1, len(unique_symbols) + 1),
            '交易币种': sorted(unique_symbols),
            '币种代码': [symbol + 'USDT' for symbol in sorted(unique_symbols)]
        })
        
        # 生成输出文件名
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(file_path), f"交易币种分析结果_{now}.xlsx")
        
        # 保存结果到Excel
        result_df.to_excel(output_path, index=False)
        print(f"\n分析结果已保存到: {output_path}")
        
        return unique_symbols, output_path
        
    except Exception as e:
        print(f"分析文件时出错: {str(e)}")
        return None

if __name__ == "__main__":
    # 文件路径
    file_path = r"C:\Users\Admin\Desktop\result.xlsx"
    analyze_trading_symbols(file_path) 