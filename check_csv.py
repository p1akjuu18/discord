#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime

def check_csv_file(file_path):
    """检查CSV文件内容，分析为什么不符合订单导入条件"""
    print(f"开始分析CSV文件: {file_path}")
    print("=" * 50)
    
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 - {file_path}")
        return
    
    # 获取文件信息
    file_size = os.path.getsize(file_path) / 1024
    mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    print(f"文件大小: {file_size:.2f} KB")
    print(f"修改时间: {mod_time}")
    print("=" * 50)
    
    try:
        # 读取CSV文件
        df = pd.read_csv(file_path)
        row_count = len(df)
        col_count = len(df.columns)
        print(f"CSV文件包含 {row_count} 行, {col_count} 列")
        
        # 显示所有列名
        print("\n所有列名:")
        for i, col in enumerate(df.columns):
            print(f"{i+1}. {col}")
        
        # 检查入场点位列
        entry_cols = [col for col in df.columns if '入场' in col or 'entry' in col.lower()]
        print(f"\n找到的入场点位相关列: {entry_cols}")
        
        # 检查止损点位列
        stop_cols = [col for col in df.columns if '止损' in col or 'stop' in col.lower() or 'sl' in col.lower()]
        print(f"找到的止损点位相关列: {stop_cols}")
        
        # 检查需要的具体列是否存在
        target_columns = ['analysis.入场点位1', 'analysis.止损点位1']
        missing_columns = [col for col in target_columns if col not in df.columns]
        
        if missing_columns:
            print(f"\n警告: 缺少以下目标列: {missing_columns}")
        else:
            print("\n全部目标列已找到")
        
        # 如果找到了相关的列，分析数据情况
        if entry_cols and stop_cols:
            # 使用第一个找到的入场和止损列
            entry_col = entry_cols[0]
            stop_col = stop_cols[0]
            
            # 检查非空值
            entry_not_null = df[entry_col].notna().sum()
            stop_not_null = df[stop_col].notna().sum()
            both_not_null = df[df[entry_col].notna() & df[stop_col].notna()].shape[0]
            
            print(f"\n数据分析结果:")
            print(f"入场点位列 '{entry_col}' 有效值数量: {entry_not_null}/{row_count}")
            print(f"止损点位列 '{stop_col}' 有效值数量: {stop_not_null}/{row_count}")
            print(f"同时有入场点位和止损点位的行数: {both_not_null}/{row_count}")
            
            # 数据类型检查
            print(f"\n数据类型检查:")
            print(f"入场点位列数据类型: {df[entry_col].dtype}")
            print(f"止损点位列数据类型: {df[stop_col].dtype}")
            
            # 尝试转换数据类型
            if df[entry_col].dtype == 'object' or df[stop_col].dtype == 'object':
                print("\n尝试将字符串转换为数值:")
                try:
                    entry_converted = pd.to_numeric(df[entry_col], errors='coerce')
                    stop_converted = pd.to_numeric(df[stop_col], errors='coerce')
                    
                    entry_valid = entry_converted.notna().sum()
                    stop_valid = stop_converted.notna().sum()
                    both_valid = (entry_converted.notna() & stop_converted.notna()).sum()
                    
                    print(f"转换后入场点位有效数值: {entry_valid}/{entry_not_null}")
                    print(f"转换后止损点位有效数值: {stop_valid}/{stop_not_null}")
                    print(f"转换后同时有有效入场点位和止损点位的行数: {both_valid}/{row_count}")
                    
                    if both_valid == 0:
                        print("\n问题诊断: 没有行同时具有有效的入场点位和止损点位数值")
                except Exception as e:
                    print(f"转换失败: {e}")
            
            # 显示有效数据的入场和止损点位
            valid_rows = df[df[entry_col].notna() & df[stop_col].notna()]
            if len(valid_rows) > 0:
                print("\n有效数据示例 (前3行):")
                for i, row in valid_rows.head(3).iterrows():
                    entry_val = row[entry_col]
                    stop_val = row[stop_col]
                    symbol = row.get('analysis.交易币种', 'Unknown')
                    direction = row.get('analysis.方向', 'Unknown')
                    
                    print(f"行 {i+1}: 币种={symbol}, 方向={direction}, 入场={entry_val}, 止损={stop_val}")
                    print(f"     类型: 入场=({type(entry_val).__name__}), 止损=({type(stop_val).__name__})")
                    
                    # 尝试转换为浮点数
                    try:
                        entry_float = float(entry_val) if pd.notna(entry_val) else None
                        stop_float = float(stop_val) if pd.notna(stop_val) else None
                        print(f"     转换后: 入场={entry_float}, 止损={stop_float}")
                    except Exception as e:
                        print(f"     转换失败: {e}")
            else:
                print("\n没有找到同时有入场点位和止损点位的行")
                
                # 显示前5行数据样本
                print("\n原始数据样本 (前5行):")
                sample_columns = ['analysis.交易币种', 'analysis.方向', entry_col, stop_col]
                sample_df = df[sample_columns].head()
                print(sample_df)
        
        # 获取币种列
        symbol_cols = [col for col in df.columns if '币种' in col or '交易' in col.lower() or 'symbol' in col.lower()]
        if symbol_cols:
            symbol_col = symbol_cols[0]
            symbols = df[symbol_col].dropna().unique()
            print(f"\n找到的币种相关列: {symbol_cols}")
            print(f"包含的交易币种: {symbols}")
            
            # 检查BTC, ETH, SOL行的数据
            target_symbols = ['BTC', 'ETH', 'SOL']
            for symbol in target_symbols:
                symbol_rows = df[df[symbol_col].str.contains(symbol, na=False, case=False)]
                if len(symbol_rows) > 0:
                    print(f"\n{symbol}相关行 ({len(symbol_rows)}行):")
                    for i, row in symbol_rows.iterrows():
                        entry_val = row.get(entry_col)
                        stop_val = row.get(stop_col)
                        print(f"行 {i+1}: 入场={entry_val}, 止损={stop_val}")
                else:
                    print(f"\n没有找到{symbol}相关行")
        
        print("\n诊断建议:")
        if missing_columns:
            print(f"1. 目标列 {missing_columns} 不存在，程序无法找到匹配的数据")
            if entry_cols and stop_cols:
                print(f"   推荐替代列: 入场 = {entry_cols[0]}, 止损 = {stop_cols[0]}")
        elif both_not_null == 0:
            print("1. 没有行同时包含有效的入场点位和止损点位数值")
            print("   可能原因: 数据格式不正确、值为空或无法转换为数值")
        else:
            print(f"1. 找到 {both_not_null} 行有效数据，但程序仍未导入")
            print("   可能原因: 数据类型转换失败或其他必要条件未满足")
            
            # 检查BTC, ETH, SOL
            for symbol in ['BTC', 'ETH', 'SOL']:
                if symbol not in str(symbols):
                    print(f"2. CSV文件中缺少有效的{symbol}数据")
                    print(f"   监控程序只支持BTC, ETH, SOL币种，但您的数据中没有有效的{symbol}行")
        
        print("\n解决方案:")
        print("1. 修改价格监控程序以适配您的CSV格式")
        print("2. 调整CSV文件格式以符合程序要求")
        print("3. 可以尝试修改以下内容:")
        print("   a. 确保CSV文件中包含BTC、ETH或SOL的交易币种数据")
        print("   b. 确保入场点位和止损点位为有效的数值")
        print("   c. 自行修改price_order_monitor.py的monitor_csv_file函数，让它能够识别您的数据格式")
        
    except Exception as e:
        print(f"解析CSV文件时出错: {e}")

if __name__ == "__main__":
    # 要检查的CSV文件路径
    csv_file_path = "data/analysis_results/all_analysis_results.csv"
    check_csv_file(csv_file_path) 