import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
import ast
from datetime import datetime
import json
import glob
import argparse
import pytz

# 列名映射字典
COLUMN_NAME_MAPPING = {
    # 主要分类
    'entry_results': '入场结果',
    'entry_points_info': '入场点信息',
    'total_profit_details': '总收益详情',
    'total': '总计',
    
    # 入场结果相关
    'entry_point': '入场点',
    'entry_price': '入场价格',
    'weight': '权重',
    'status': '状态',
    'entry_hit': '入场命中',
    'actual_entry_time': '实际入场时间',
    'outcome': '结果',
    'tp_results': '止盈结果',
    'tp_price': '止盈价格',
    'tp_weight': '止盈权重',
    'profit_pct': '收益率',
    'exit_time': '退出时间',
    'step': '步骤',
    'total_profit_pct': '总收益率',
    'remaining_weight': '剩余权重',
    'exit_price': '退出价格',
    'weighted_profit': '加权收益',
    'holding_period_minutes': '持仓时间',
    'risk_reward_ratio': '风险收益比',
    
    # 入场点信息相关
    'target_price': '目标价格',
    'actual_entry': '实际入场',
    'entry_time': '入场时间',
    'actual_price': '实际价格',
    
    # 收益详情相关
    'tp': '止盈',
    'sl': '止损',
    'open': '开仓',
    'profit': '收益',
    'details': '详情',
    
    # 通用字段
    'price': '价格',
    'time': '时间',
    'volume': '成交量',
    'side': '方向',
    'position': '仓位'
}

def get_chinese_column_name(eng_name):
    """
    将英文列名转换为中文列名
    """
    parts = eng_name.split('_')
    translated_parts = []
    i = 0
    
    while i < len(parts):
        # 处理数字部分
        if parts[i].isdigit():
            translated_parts.append(parts[i])
            i += 1
            continue
            
        # 尝试组合多个部分进行翻译
        for j in range(len(parts), i, -1):
            combined = '_'.join(parts[i:j])
            if combined in COLUMN_NAME_MAPPING:
                translated_parts.append(COLUMN_NAME_MAPPING[combined])
                i = j
                break
        else:
            # 如果没有找到组合匹配，尝试单个部分
            if parts[i] in COLUMN_NAME_MAPPING:
                translated_parts.append(COLUMN_NAME_MAPPING[parts[i]])
            else:
                translated_parts.append(parts[i])
            i += 1
    
    return ''.join(translated_parts)

def clean_numeric_value(value):
    """
    清理数值，去除可能残留的字典或列表结束符号
    """
    if isinstance(value, str):
        # 去除结尾可能的 } 或 ]
        value = value.rstrip('}]')
        try:
            return float(value)
        except:
            return value
    return value

def process_backtest_results(file_path):
    """
    读取回测结果文件并处理嵌套数据结构
    """
    # 确定文件类型并读取
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("不支持的文件格式，请提供CSV或Excel文件")
    
    # 复制原始数据框
    processed_df = df.copy()
    invalid_df = df.copy()  # 创建一个副本用于存储无效数据
    
    # 需要处理的嵌套列名
    nested_columns = ['entry_results', 'entry_points_info', 'total_profit_details']
    
    # 处理每个嵌套列
    for col in nested_columns:
        if col in df.columns:
            try:
                # 展平嵌套数据
                processed_df = flatten_nested_column(processed_df, col)
            except Exception as e:
                print(f"处理列 {col} 时出错: {e}")
    
    # 保存channel列的原始值（如果存在）
    channel_col = None
    if 'channel' in processed_df.columns:
        channel_col = processed_df['channel'].copy()
    
    # 转换所有列名为中文
    processed_df.columns = [get_chinese_column_name(col) for col in processed_df.columns]
    
    # 如果之前存在channel列，恢复它
    if channel_col is not None:
        processed_df['channel'] = channel_col
    
    # 清理所有包含"风险收益比"、"实际价格"或"持仓时间"的列
    for col in processed_df.columns:
        if '风险收益比' in col or '实际价格' in col or '持仓时间' in col:
            processed_df[col] = processed_df[col].apply(clean_numeric_value)
    
    # 分离有效和无效数据
    # 检查是否存在关键列来判断数据是否有效
    key_columns = ['入场点位1', '止损点位1', '止盈点位1', '方向']
    has_valid_data = False
    
    for col in key_columns:
        if col in processed_df.columns:
            has_valid_data = True
            # 将所有非空值的行标记为有效数据
            valid_mask = processed_df[col].notna()
            processed_df = processed_df[valid_mask]
            invalid_df = invalid_df[~valid_mask]
            break
    
    if not has_valid_data:
        print("警告：未找到任何关键列来判断数据有效性")
        return processed_df, None
    
    return processed_df, invalid_df

def direct_parser(s):
    """
    直接解析嵌套数据结构的字符串表示
    """
    if not isinstance(s, str):
        return s
    
    # 检查是否为字典格式（单个字典而非列表）
    if s.startswith('{') and s.endswith('}'):
        try:
            # 解析单个字典
            dict_obj = {}
            
            # 使用正则表达式匹配键值对
            key_value_pattern = r"'([^']+)':\s*([^,]+)"
            pairs = re.findall(key_value_pattern, s)
            
            for key, value in pairs:
                # 处理不同类型的值
                if value.strip() == 'True':
                    dict_obj[key] = True
                elif value.strip() == 'False':
                    dict_obj[key] = False
                elif value.strip() == 'None':
                    dict_obj[key] = None
                elif 'np.float64' in value:
                    # 提取np.float64中的数值
                    match = re.search(r'np\.float64\(([^)]+)\)', value)
                    if match:
                        dict_obj[key] = float(match.group(1))
                    else:
                        dict_obj[key] = value
                elif 'Timestamp' in value:
                    # 提取Timestamp中的日期时间
                    match = re.search(r"Timestamp\('([^']+)'\)", value)
                    if match:
                        try:
                            dict_obj[key] = datetime.fromisoformat(match.group(1))
                        except:
                            dict_obj[key] = match.group(1)
                    else:
                        dict_obj[key] = value
                else:
                    # 尝试转换为数值
                    try:
                        dict_obj[key] = float(value)
                    except:
                        dict_obj[key] = value.strip("' ")
                        
            return dict_obj
        except Exception as e:
            print(f"解析字典错误: {e}")
            return s
    
    # 检查是否为列表格式的字符串
    if not (s.startswith('[') and s.endswith(']')):
        return s
    
    try:
        # 使用正则表达式识别并提取字典对象
        dict_pattern = r'\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
        dicts_found = re.findall(dict_pattern, s)
        
        result_list = []
        
        for dict_str in dicts_found:
            # 解析每个字典
            dict_obj = {}
            
            # 简化: 使用正则表达式匹配键值对
            key_value_pattern = r"'([^']+)':\s*([^,]+)"
            pairs = re.findall(key_value_pattern, '{' + dict_str + '}')
            
            for key, value in pairs:
                # 处理不同类型的值
                if value.strip() == 'True':
                    dict_obj[key] = True
                elif value.strip() == 'False':
                    dict_obj[key] = False
                elif value.strip() == 'None':
                    dict_obj[key] = None
                elif 'np.float64' in value:
                    # 提取np.float64中的数值
                    match = re.search(r'np\.float64\(([^)]+)\)', value)
                    if match:
                        dict_obj[key] = float(match.group(1))
                    else:
                        dict_obj[key] = value
                elif 'Timestamp' in value:
                    # 提取Timestamp中的日期时间
                    match = re.search(r"Timestamp\('([^']+)'\)", value)
                    if match:
                        try:
                            dict_obj[key] = datetime.fromisoformat(match.group(1))
                        except:
                            dict_obj[key] = match.group(1)
                    else:
                        dict_obj[key] = value
                else:
                    # 尝试转换为数值
                    try:
                        dict_obj[key] = float(value)
                    except:
                        dict_obj[key] = value.strip("' ")
            
            result_list.append(dict_obj)
        
        return result_list
    except Exception as e:
        print(f"解析列表错误: {e}")
        return s

def flatten_nested_column(df, col_name):
    """
    将嵌套列展平为多个单独的列
    """
    # 检查列是否存在且包含数据
    if col_name not in df.columns or df[col_name].isnull().all():
        return df
    
    # 使用直接解析器解析嵌套数据
    df[col_name] = df[col_name].apply(direct_parser)
    
    # 打印一些解析后的样本，用于调试
    print(f"解析后的 {col_name} 样本:")
    first_valid_index = df[col_name].first_valid_index()
    if first_valid_index is not None:
        print(f"类型: {type(df.loc[first_valid_index, col_name])}")
        print(df.loc[first_valid_index, col_name])
    
    # 处理单个字典的情况
    if df[col_name].apply(lambda x: isinstance(x, dict)).any():
        print(f"检测到 {col_name} 列包含字典数据，正在处理...")
        # 获取一个样本字典用于提取键
        sample_dict = df.loc[df[col_name].apply(lambda x: isinstance(x, dict))].iloc[0][col_name]
        
        # 为字典中的每个键创建新列
        for key in sample_dict.keys():
            new_col_name = f"{col_name}_{key}"
            df[new_col_name] = df[col_name].apply(lambda x: x.get(key) if isinstance(x, dict) else None)
            # 打印创建的列信息
            print(f"创建列 {new_col_name}, 样本值: {df[new_col_name].iloc[0]}")
        
        # 删除原始字典列
        df = df.drop(columns=[col_name])
        return df
    
    # 处理列表数据
    valid_list_entries = df[col_name].apply(lambda x: isinstance(x, list) and len(x) > 0)
    if not valid_list_entries.any():
        print(f"警告: {col_name} 列没有有效的列表数据")
        return df
    
    # 获取第一个有效的列表元素作为模板
    template_index = valid_list_entries[valid_list_entries].index[0]
    first_elem = df.loc[template_index, col_name]
    
    # 对于列表中的每个元素位置创建新列
    for i in range(len(first_elem)):
        prefix = f"{col_name}_{i+1}"
        
        # 创建一个函数来提取第i个元素（如果存在）
        def extract_element(row, idx):
            if isinstance(row, list) and len(row) > idx:
                return row[idx]
            return None
        
        # 为第i个元素创建新列
        df[prefix] = df[col_name].apply(lambda x: extract_element(x, i))
        
        # 如果元素是字典，则为字典中的每个键创建新列
        sample_elem = extract_element(first_elem, i)
        if isinstance(sample_elem, dict):
            for key in sample_elem.keys():
                new_col_name = f"{prefix}_{key}"
                df[new_col_name] = df[prefix].apply(lambda x: x.get(key) if isinstance(x, dict) else None)
                # 打印创建的列信息
                print(f"创建列 {new_col_name}, 样本值: {df[new_col_name].iloc[0]}")
            
            # 删除中间字典列
            df = df.drop(columns=[prefix])
    
    # 删除原始嵌套列
    df = df.drop(columns=[col_name])
    
    return df

# 以下为模块1: 回测结果处理函数
def process_backtest_results_main():
    # 获取桌面路径
    desktop_path = Path(os.path.join(os.path.expanduser("~"), "Desktop"))
    
    # 固定文件名为"回测结果.xlsx"
    file_name = "回测结果.xlsx"
    output_file_name = f"processed_{file_name}"
    invalid_file_name = f"unprocessed_{file_name}"
    
    file_path = desktop_path / file_name
    output_path = desktop_path / output_file_name
    invalid_path = desktop_path / invalid_file_name
    
    if not file_path.exists():
        print(f"文件 {file_path} 不存在!")
        return
    
    # 处理文件
    try:
        # 如果输出文件已存在，尝试删除它们
        for path in [output_path, invalid_path]:
            if path.exists():
                try:
                    os.remove(path)
                    print(f"已删除现有的输出文件: {path}")
                except Exception as e:
                    print(f"无法删除现有的输出文件: {e}")
                    print("请确保文件未被其他程序打开，并且您有足够的权限。")
                    return
        
        # 读取原始文件用于统计列数
        if file_path.suffix == '.csv':
            original_df = pd.read_csv(file_path)
        else:
            original_df = pd.read_excel(file_path)
            
        # 处理文件
        result_df, invalid_df = process_backtest_results(str(file_path))
        
        # 尝试保存处理后的有效数据
        temp_output_path = desktop_path / f"temp_{output_file_name}"
        if file_path.suffix == '.csv':
            result_df.to_csv(temp_output_path, index=False)
        else:
            result_df.to_excel(temp_output_path, index=False)
        
        # 如果有无效数据，保存到单独的文件
        if invalid_df is not None and not invalid_df.empty:
            temp_invalid_path = desktop_path / f"temp_{invalid_file_name}"
            if file_path.suffix == '.csv':
                invalid_df.to_csv(temp_invalid_path, index=False)
            else:
                invalid_df.to_excel(temp_invalid_path, index=False)
        
        # 如果临时文件保存成功，重命名为最终文件名
        try:
            if temp_output_path.exists():
                if output_path.exists():
                    os.remove(output_path)
                os.rename(temp_output_path, output_path)
                print(f"有效数据已保存至：{output_path}")
                print(f"原始列数: {len(original_df.columns)}, 处理后列数: {len(result_df.columns)}")
            
            if invalid_df is not None and not invalid_df.empty and temp_invalid_path.exists():
                if invalid_path.exists():
                    os.remove(invalid_path)
                os.rename(temp_invalid_path, invalid_path)
                print(f"未处理的数据已保存至：{invalid_path}")
                print(f"未处理数据行数: {len(invalid_df)}")
        except Exception as e:
            print(f"重命名文件时出错: {e}")
            if temp_output_path.exists():
                print(f"处理后的文件已保存为临时文件: {temp_output_path}")
            if invalid_df is not None and not invalid_df.empty and temp_invalid_path.exists():
                print(f"未处理的数据已保存为临时文件: {temp_invalid_path}")
            
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

# 模块2: Discord消息处理函数
def process_discord_messages_main():
    # 读取JSON文件
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    target_file = os.path.join(desktop, '𝑾𝑾𝑮-𝑬𝑳𝑰𝒁 [1224357517321048248].json')
    
    if not os.path.exists(target_file):
        print(f"错误：找不到文件 {target_file}")
        return
    
    try:
        # 读取JSON文件
        with open(target_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 筛选消息
        filtered_messages = []
        for message in data.get('messages', []):
            # 检查是否包含表情或者特定内容
            has_emojis = bool(message.get('inlineEmojis'))
            has_strategy = 'Eliz交易策略' in message.get('content', '')
            
            if has_emojis or has_strategy:
                filtered_message = {
                    'id': message['id'],
                    'timestamp': message['timestamp'],
                    'content': message['content'],
                    'author': message['author']['name'],
                    'has_emojis': has_emojis,
                    'has_strategy': has_strategy,
                    'inlineEmojis': message.get('inlineEmojis', [])
                }
                filtered_messages.append(filtered_message)
        
        # 将结果保存到桌面
        output_file = os.path.join(desktop, 'filtered_messages.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_messages, f, ensure_ascii=False, indent=2)
        
        # 统计信息
        emoji_count = sum(1 for msg in filtered_messages if msg['has_emojis'])
        strategy_count = sum(1 for msg in filtered_messages if msg['has_strategy'])
        
        print(f"筛选结果：")
        print(f"- 包含表情的消息：{emoji_count} 条")
        print(f"- 包含交易策略的消息：{strategy_count} 条")
        print(f"- 总共筛选出：{len(filtered_messages)} 条消息")
        print(f"结果已保存到：{output_file}")
    
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

# 模块3: 加密货币市场数据准备
def prepare_market_data_main():
    crypto_folder_path = input("请输入加密货币数据文件夹路径 (默认为桌面上的crypto文件夹): ")
    if not crypto_folder_path:
        desktop_path = os.path.expanduser('~') + '/Desktop'
        crypto_folder_path = os.path.join(desktop_path, 'crypto')
    
    output_file = input("请输入输出文件名 (默认为market_data.csv): ")
    if not output_file:
        output_file = 'market_data.csv'
    
    if not os.path.exists(crypto_folder_path):
        print(f"错误: 文件夹 {crypto_folder_path} 不存在")
        return
    
    try:
        result = prepare_market_data(crypto_folder_path, output_file)
        if result is not None:
            print(f"处理完成，共合并 {len(result['symbol'].unique())} 种加密货币的数据")
    except Exception as e:
        print(f"处理数据时出错: {e}")
        import traceback
        traceback.print_exc()

# 模块4: Excel分析结构修复
def fix_analysis_structure_main():
    # 设置桌面上的result3文件
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    target_file = os.path.join(desktop_path, "result3.xlsx")
    output_file = os.path.join(desktop_path, "result3_fixed_analysis.xlsx")
    
    if not os.path.exists(target_file):
        print(f"文件 {target_file} 不存在!")
        return
    
    try:
        # 读取Excel文件
        df = pd.read_excel(target_file)
        
        # 处理数据
        processed_df = process_excel_structure(df)
        
        # 保存处理后的文件
        processed_df.to_excel(output_file, index=False)
        print(f"文件处理完成，已保存到: {output_file}")
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

def process_excel_structure(df):
    """
    处理Excel文件的结构问题
    """
    # 复制数据框
    processed_df = df.copy()
    
    # 处理列名
    processed_df.columns = [col.strip() for col in processed_df.columns]
    
    # 处理空值
    processed_df = processed_df.fillna('')
    
    # 处理数据类型
    for col in processed_df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            try:
                processed_df[col] = pd.to_datetime(processed_df[col])
            except:
                pass
    
    return processed_df

# 模块5: 加密货币数据处理与时间缺口检查
def process_crypto_data_main():
    # 定义数据源路径（桌面上的crypto_data文件夹）
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    data_folder = os.path.join(desktop_path, 'crypto_data')
    
    # 询问用户是否要自定义目录
    custom_dir = input(f"默认处理目录为: {data_folder}\n是否要自定义目录? (y/n, 默认n): ").lower()
    if custom_dir == 'y':
        data_folder = input("请输入目录路径: ")
        if not os.path.exists(data_folder):
            print(f"目录 {data_folder} 不存在!")
            return
    
    output_folder = os.path.join(data_folder, 'processed_data')
    
    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # 要处理的加密货币列表
    crypto_files = []
    custom_crypto = input("请输入要处理的加密货币文件名，用逗号分隔 (默认为btcusdt_history,ethusdt_history,solusdt_history): ")
    if custom_crypto:
        crypto_files = [name.strip() for name in custom_crypto.split(',')]
    else:
        crypto_files = [
            'btcusdt_history',
            'ethusdt_history',
            'solusdt_history'
        ]
    
    # 处理所有加密货币数据
    all_results = []
    for crypto_file in crypto_files:
        result = process_crypto_data(crypto_file)
        if result:
            all_results.append(result)
    
    # 生成汇总报告
    print("\n===== 数据完整性报告 =====")
    all_gaps = []
    
    for result in all_results:
        filename = result["filename"]
        start_date = result["start_date"]
        end_date = result["end_date"]
        gaps = result["gaps"]
        
        print(f"\n{filename}:")
        print(f"  数据范围: {start_date} 至 {end_date}")
        print(f"  缺失数量: {len(gaps)} 处")
        
        if gaps:
            total_missing_minutes = sum(minutes for _, _, minutes in gaps)
            total_duration = (end_date - start_date).total_seconds() / 60
            missing_percentage = (total_missing_minutes / total_duration) * 100
            
            print(f"  总缺失时间: {total_missing_minutes} 分钟 (约 {missing_percentage:.2f}% 的数据)")
            print("\n  详细缺失时段:")
            for i, (start, end, minutes) in enumerate(gaps):
                print(f"    {i+1}. {start} 至 {end} (约 {minutes} 分钟)")
                # 收集所有缺失数据用于表格输出
                all_gaps.append({
                    "币种": filename,
                    "开始时间": start,
                    "结束时间": end,
                    "缺失分钟数": minutes
                })
    
    # 将缺失数据保存到CSV文件
    if all_gaps:
        # 创建DataFrame
        gaps_df = pd.DataFrame(all_gaps)
        
        # 保存到输出文件夹中的CSV文件
        gaps_csv_path = os.path.join(output_folder, f"crypto_data_gaps.csv")
        gaps_df.to_csv(gaps_csv_path, index=False, encoding='utf-8-sig')
        
        print(f"\n已将缺失数据保存到表格: {gaps_csv_path}")
    else:
        print("\n所有数据完整，无缺失。")
    
    print("\n所有数据处理完成！数据已保存到:", output_folder)

# 模块6: 合并表文件处理程序
def process_merged_tables_main():
    # 设置文件夹路径
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    data_dir = os.path.join(desktop_path, "historydata")
    
    # 询问用户是否要自定义目录
    custom_dir = input(f"默认处理目录为: {data_dir}\n是否要自定义目录? (y/n, 默认n): ").lower()
    if custom_dir == 'y':
        data_dir = input("请输入目录路径: ")
    
    # 确保目录存在
    if not os.path.exists(data_dir):
        print(f"错误: 目录 {data_dir} 不存在!")
        os.makedirs(data_dir)
        print(f"已创建目录: {data_dir}")
        print("请将数据文件放入该目录，然后重新运行程序。")
        return
    
    # 列出所有文件
    files = list_files(data_dir)
    
    if not files:
        print("没有找到文件!")
        return
    
    # 让用户选择文件
    while True:
        try:
            choice = int(input("\n请选择要处理的文件编号 (0退出): "))
            if choice == 0:
                return
            if 1 <= choice <= len(files):
                break
            print(f"请输入1到{len(files)}之间的数字!")
        except ValueError:
            print("请输入有效的数字!")
    
    selected_file = files[choice-1]
    print(f"\n已选择: {selected_file}")
    
    # 读取数据
    df = read_file(selected_file)
    
    # 备份原始数据，用于后续提取方向为NaN的行
    original_df = df.copy() if df is not None else None
    
    # 处理数据
    processed_df = process_data(df)
    
    if processed_df is not None:
        # 生成默认的输出文件名（基于原始文件名）
        original_filename = os.path.basename(selected_file)
        file_name, file_ext = os.path.splitext(original_filename)
        if not file_ext or file_ext.lower() not in ['.csv', '.xlsx', '.xls']:
            file_ext = '.csv'  # 默认CSV格式
        
        default_output_name = f"{file_name}_processed{file_ext}"
        
        # 询问用户是否要保存处理后的数据（默认是）
        save_choice = input(f"\n是否要保存处理后的数据到新文件? (y/n, 默认y): ").lower()
        if save_choice == '' or save_choice == 'y':
            output_name = input(f"请输入输出文件名 (默认: {default_output_name}): ")
            if not output_name:
                output_name = default_output_name
            
            # 确保文件名有正确的扩展名
            if not any(output_name.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                output_name += file_ext
            
            output_path = os.path.join(data_dir, output_name)
            
            # 检查文件是否已存在
            if os.path.exists(output_path):
                overwrite = input(f"文件 {output_name} 已存在。是否覆盖? (y/n): ").lower()
                if overwrite != 'y':
                    new_name = input("请输入新的文件名: ")
                    if not new_name:
                        print("未保存文件")
                        return
                    
                    # 确保文件名有正确的扩展名
                    if not any(new_name.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                        new_name += file_ext
                    
                    output_path = os.path.join(data_dir, new_name)
            
            save_processed_data(processed_df, output_path)
            print(f"\n处理完成! 原始文件保持不变，处理后的数据已保存到新文件: {os.path.basename(output_path)}")
        else:
            print("\n数据未保存")
        
        # 处理方向列为NaN的行
        if original_df is not None:
            direction_column = None
            possible_direction_columns = ['方向', '交易方向', '买卖方向', '多空', 'direction', 'Direction']
            
            for col in possible_direction_columns:
                if col in original_df.columns:
                    direction_column = col
                    break
            
            if direction_column:
                # 查找方向列为NaN的行
                nan_direction_rows = original_df[original_df[direction_column].isna()]
                
                # 如果存在方向列为NaN的行，询问用户是否要单独保存
                if not nan_direction_rows.empty:
                    print(f"\n发现 {len(nan_direction_rows)} 行的交易方向列 '{direction_column}' 为空值")
                    save_nan_choice = input("是否要将这些行单独保存到一个文件? (y/n, 默认y): ").lower()
                    
                    if save_nan_choice == '' or save_nan_choice == 'y':
                        nan_output_name = f"{file_name}_direction_empty{file_ext}"
                        nan_output_name = input(f"请输入保存方向为空的行的文件名 (默认: {nan_output_name}): ") or nan_output_name
                        
                        # 确保文件名有正确的扩展名
                        if not any(nan_output_name.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                            nan_output_name += file_ext
                        
                        nan_output_path = os.path.join(data_dir, nan_output_name)
                        
                        # 检查文件是否已存在
                        if os.path.exists(nan_output_path):
                            overwrite = input(f"文件 {nan_output_name} 已存在。是否覆盖? (y/n): ").lower()
                            if overwrite != 'y':
                                new_name = input("请输入新的文件名: ")
                                if not new_name:
                                    print("未保存方向为空的行")
                                    return
                                
                                # 确保文件名有正确的扩展名
                                if not any(new_name.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                                    new_name += file_ext
                                
                                nan_output_path = os.path.join(data_dir, new_name)
                        
                        # 保存方向为NaN的行
                        save_processed_data(nan_direction_rows, nan_output_path)
                        print(f"方向为空的行已保存到: {os.path.basename(nan_output_path)}")
                else:
                    print("\n未发现交易方向列为空的行")
            else:
                print("\n未找到交易方向列，无法提取方向为空的行")

# 模块7: Excel嵌套数据修复
def fix_nested_json_main():
    # 设置桌面上的result3文件
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    target_file = os.path.join(desktop_path, "result3.xlsx")
    output_file = os.path.join(desktop_path, "result3_fixed_nested.xlsx")
    
    if not os.path.exists(target_file):
        print(f"文件 {target_file} 不存在!")
        return
    
    try:
        # 读取Excel文件
        df = pd.read_excel(target_file)
        
        # 处理嵌套数据
        processed_df = process_excel_nested_data(df)
        
        # 保存处理后的文件
        processed_df.to_excel(output_file, index=False)
        print(f"文件处理完成，已保存到: {output_file}")
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

def process_excel_nested_data(df):
    """
    处理Excel文件中的嵌套数据
    """
    # 复制数据框
    processed_df = df.copy()
    
    # 处理可能包含嵌套数据的列
    for col in processed_df.columns:
        if isinstance(processed_df[col].iloc[0], str) and ('{' in processed_df[col].iloc[0] or '[' in processed_df[col].iloc[0]):
            try:
                # 尝试解析嵌套数据
                processed_df[col] = processed_df[col].apply(lambda x: eval(x) if isinstance(x, str) and (x.startswith('{') or x.startswith('[')) else x)
                
                # 如果成功解析，展开嵌套数据
                if isinstance(processed_df[col].iloc[0], dict):
                    new_cols = pd.json_normalize(processed_df[col])
                    processed_df = pd.concat([processed_df.drop(columns=[col]), new_cols], axis=1)
            except:
                pass
    
    return processed_df

# 主菜单函数
def main():
    while True:
        print("\n======================================================")
        print("                  数据处理工具集 v1.0                  ")
        print("======================================================")
        print("请选择要运行的模块:")
        print("1. 回测结果处理工具 - 处理回测结果数据结构")
        print("2. Discord消息筛选工具 - 筛选包含表情或特定内容的消息")
        print("3. 加密货币市场数据准备工具 - 整合多个加密货币历史数据")
        print("4. Excel分析结构修复工具 - 修复分析结构")
        print("5. 加密货币数据处理与时间缺口检查工具 - 按月拆分数据并检查缺失")
        print("6. 合并表文件处理程序 - 处理交易数据文件")
        print("7. Excel嵌套数据修复工具 - 修复嵌套的Excel数据")
        print("0. 退出程序")
        print("------------------------------------------------------")
        
        choice = input("请输入模块编号 (0-7): ").strip()
        
        if choice == '0':
            print("\n感谢使用！再见！")
            break
        elif choice == '1':
            print("\n正在启动回测结果处理工具...\n")
            process_backtest_results_main()
        elif choice == '2':
            print("\n正在启动Discord消息筛选工具...\n")
            process_discord_messages_main()
        elif choice == '3':
            print("\n正在启动加密货币市场数据准备工具...\n")
            prepare_market_data_main()
        elif choice == '4':
            print("\n正在启动Excel分析结构修复工具...\n")
            fix_analysis_structure_main()
        elif choice == '5':
            print("\n正在启动加密货币数据处理与时间缺口检查工具...\n")
            process_crypto_data_main()
        elif choice == '6':
            print("\n正在启动合并表文件处理程序...\n")
            process_merged_tables_main()
        elif choice == '7':
            print("\n正在启动Excel嵌套数据修复工具...\n")
            fix_nested_json_main()
        else:
            print("\n无效的选择，请输入0-7之间的数字")
        
        input("\n按Enter键返回主菜单...")

if __name__ == "__main__":
    main() 