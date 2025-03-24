import os
import json
import pandas as pd
import re
import time
from pathlib import Path

def flatten_json(nested_json, prefix=''):
    """
    将嵌套的JSON数据结构展平成一级结构，便于转换为Excel的列
    """
    flat_json = {}
    
    # 处理列表类型
    if isinstance(nested_json, list):
        for i, item in enumerate(nested_json):
            flat_json.update(flatten_json(item, f"{prefix}{i}_"))
    # 处理字典类型
    elif isinstance(nested_json, dict):
        for key, value in nested_json.items():
            if isinstance(value, (dict, list)):
                flat_json.update(flatten_json(value, f"{prefix}{key}_"))
            else:
                flat_json[f"{prefix}{key}"] = value
    # 其他类型直接返回
    else:
        flat_json[prefix[:-1]] = nested_json
        
    return flat_json

def merge_json_files_to_excel(directory_path, output_excel_path):
    """
    合并指定目录下的所有JSON文件，并将结果保存为Excel文件
    将嵌套的JSON字段完全展平为单独的列，保留所有细节
    
    筛选规则:
    1. 筛选掉交易币种为空的行
    2. 统一用户名下，对相同"入场点位1"只保留一个记录
    3. 删除所有入场/止盈/止损点位都为空的行
    
    参数:
        directory_path (str): JSON文件所在的目录路径
        output_excel_path (str): 输出Excel文件的路径
    """
    all_data = []
    file_count = 0
    
    # 遍历目录中的所有文件
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)
            try:
                # 读取JSON文件
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # 提取用户名（从文件名中）
                user_name = re.sub(r'_results\.json$', '', filename)
                
                # 处理嵌套的JSON结构
                processed_data = {"用户名": user_name, "源文件": filename}
                
                # 如果JSON是字典类型
                if isinstance(json_data, dict):
                    # 完全展平JSON结构，包括所有嵌套层级
                    flattened_data = flatten_json(json_data)
                    processed_data.update(flattened_data)
                    
                    # 特别处理analysis字段，保持其直接可见性
                    if 'analysis' in json_data:
                        for key, value in json_data['analysis'].items():
                            processed_data[key] = value
                
                # 如果JSON是列表类型
                elif isinstance(json_data, list):
                    # 将列表中的每个项目作为单独的记录处理
                    for index, item in enumerate(json_data):
                        item_data = flatten_json(item)
                        item_data.update({"用户名": user_name, "源文件": filename, "索引": index})
                        all_data.append(item_data)
                    continue  # 已添加到all_data，跳过下面的append
                
                all_data.append(processed_data)
                file_count += 1
                print(f"已处理文件: {filename}")
                
            except Exception as e:
                print(f"处理文件 {filename} 时出错: {e}")
    
    # 如果没有收集到数据，直接返回
    if not all_data:
        print("没有找到有效的JSON数据")
        return
    
    # 转换为DataFrame
    df = pd.DataFrame(all_data)
    
    # 记录原始行数
    original_count = len(df)
    print(f"原始数据: {original_count} 行")
    
    # 筛选1: 筛选出交易币种不为空的行
    currency_columns = ['交易币种', 'analysis_交易币种']
    for col in currency_columns:
        if col in df.columns:
            df = df[df[col].notna() & (df[col] != '')]
            print(f"已过滤掉 {col} 为空的行，剩余 {len(df)} 行")
            break
    
    # 筛选2: 统一用户名下，如果入场点位1相同，只保留一个
    entry_columns = ['入场点位1', 'analysis_入场点位1']
    entry_col = None
    for col in entry_columns:
        if col in df.columns:
            entry_col = col
            break
    
    if entry_col:
        # 将NaN值转换为特殊标记，避免分组问题
        df[entry_col] = df[entry_col].fillna('NO_ENTRY_POINT')
        # 按用户名和入场点位分组，保留第一个记录
        df = df.drop_duplicates(subset=['用户名', entry_col])
        # 将特殊标记转回NaN
        df[entry_col] = df[entry_col].replace('NO_ENTRY_POINT', pd.NA)
        print(f"已合并相同用户名下入场点位1相同的记录，剩余 {len(df)} 行")
    
    # 筛选3: 删除所有点位都为空的行
    # 只要在入场点位、止盈点位、止损点位中有任意一个点位有值，就保留该行
    position_keywords = ['入场点位', '止盈点位', '止损点位']
    position_columns = []
    
    # 收集所有与点位相关的列
    for col in df.columns:
        for keyword in position_keywords:
            if keyword in col:
                position_columns.append(col)
                break
    
    if position_columns:
        print(f"找到以下点位相关列: {', '.join(position_columns)}")
        
        # 创建一个布尔序列，标记每行是否所有点位列都为空
        # isna() 检查是否为NA, 然后用 all(axis=1) 检查行中是否所有点位列都是NA
        all_na = df[position_columns].isna().all(axis=1)
        
        # 同样检查是否所有点位列都是空字符串
        all_empty_str = (df[position_columns] == '').all(axis=1)
        
        # 结合两个条件：所有点位都是NA 或 所有点位都是空字符串
        all_positions_empty = all_na | all_empty_str
        
        # ~all_positions_empty 表示"不是所有点位都为空"，即"至少有一个点位有值"
        df = df[~all_positions_empty]
        
        # 输出清晰的筛选结果
        filtered_rows = sum(all_positions_empty)
        print(f"已删除 {filtered_rows} 行（所有点位都为空的行）")
        print(f"保留了 {len(df)} 行（至少有一个点位有值的行）")
    
    # 记录过滤后的行数和总过滤数量
    final_count = len(df)
    filtered_count = original_count - final_count
    print(f"总过滤结果: 原始 {original_count} 行, 最终 {final_count} 行, 移除了 {filtered_count} 行")
    
    # 如果过滤后没有数据，直接返回
    if final_count == 0:
        print("过滤后没有剩余数据，无法生成Excel文件")
        return
    
    # 标准化交易方向
    direction_columns = ['方向', 'analysis_方向']
    for col in direction_columns:
        if col in df.columns:
            # 统一多头表示
            long_terms = ['做多', '多单', '多', 'long', 'buy', '买入', '看多']
            # 统一空头表示
            short_terms = ['做空', '空单', '空', 'short', 'sell', '卖出', '看空']
            
            # 创建方向映射函数
            def standardize_direction(value):
                if pd.isna(value) or value == '':
                    return value
                
                value_lower = str(value).lower()  # 转为小写便于比较
                
                for term in long_terms:
                    if term.lower() in value_lower:
                        return '做多'
                
                for term in short_terms:
                    if term.lower() in value_lower:
                        return '做空'
                
                return value  # 如果没有匹配，保持原值
            
            # 应用标准化
            df[col] = df[col].apply(standardize_direction)
            print(f"已标准化 {col} 列的交易方向: 多头统一为'做多'，空头统一为'做空'")
            
            # 记录统计
            direction_stats = df[col].value_counts().to_dict()
            print(f"方向分布: {direction_stats}")
            break  # 只处理找到的第一个方向列
    
    # 标准化交易方向后，清理交易币种名称
    currency_columns = ['交易币种', 'analysis_交易币种']
    for col in currency_columns:
        if col in df.columns:
            # 定义清理函数，处理币种名称标准化
            def clean_currency(value):
                if pd.isna(value) or value == '':
                    return value
                
                # 转为字符串并去除空格
                value_str = str(value).strip()
                
                # 币种名称映射字典
                currency_mapping = {
                    '大饼': 'BTC',
                    '比特币': 'BTC',
                    'Bitcoin': 'BTC',
                    'bitcoin': 'BTC',
                    'BITCOIN': 'BTC',
                    '二饼': 'ETH',
                    '以太坊': 'ETH',
                    'Ethereum': 'ETH',
                    'ethereum': 'ETH',
                    'ETHEREUM': 'ETH'
                }
                
                # 检查是否有精确匹配
                if value_str in currency_mapping:
                    value_str = currency_mapping[value_str]
                else:
                    # 模糊匹配，检查是否包含这些关键词
                    for keyword, replacement in currency_mapping.items():
                        if keyword in value_str:
                            value_str = replacement
                            break
                
                # 检查并删除末尾的 /USDT 或变体
                patterns = ['/usdt', '/USDT', '/Usdt']
                for pattern in patterns:
                    if value_str.endswith(pattern):
                        return value_str[:-len(pattern)]
                
                return value_str
            
            # 应用清理函数
            original_values = df[col].copy()
            df[col] = df[col].apply(clean_currency)
            
            # 计算有多少值被修改了
            changed_count = (original_values != df[col]).sum()
            print(f"已标准化 {col} 列中的 {changed_count} 个交易币种名称")
            print("- 已将'大饼'相关描述统一为'BTC'")
            print("- 已将'二饼'相关描述统一为'ETH'")
            print("- 已移除'/USDT'等后缀")
            
            # 输出最终的币种分布
            currency_stats = df[col].value_counts().head(10).to_dict()
            print(f"前10种交易币种分布: {currency_stats}")
            break  # 只处理找到的第一个交易币种列
    
    # 调整列的顺序，确保重要字段在前面
    important_columns = ['用户名', '源文件', '交易币种', '方向', '杠杆', 
                         '入场点位1', '入场点位2', '入场点位3',
                         '止损点位1', '止损点位2', '止损点位3',
                         '止盈点位1', '止盈点位2', '止盈点位3',
                         '分析内容', '原文', '翻译']
    
    # 获取所有列
    all_columns = df.columns.tolist()
    
    # 将重要列排在前面，其余列保持原顺序
    ordered_columns = [col for col in important_columns if col in all_columns]
    remaining_columns = [col for col in all_columns if col not in important_columns]
    final_columns = ordered_columns + remaining_columns
    
    # 重新排序DataFrame的列
    df = df[final_columns]
    
    # 设置输出文件路径
    output_path = Path(output_excel_path)
    
    # 尝试保存文件，如果遇到权限错误则换个文件名
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            # 检查文件是否存在并且可能被占用
            if output_path.exists():
                # 创建一个带时间戳的备用文件名
                timestamp = int(time.time())
                new_path = output_path.with_stem(f"{output_path.stem}_{timestamp}")
                print(f"文件可能被占用，尝试保存到: {new_path}")
                output_path = new_path
            
            # 直接使用to_excel方法保存
            df.to_excel(output_path, index=False, engine='openpyxl')
            print(f"已将合并数据保存至: {output_path}")
            break
            
        except PermissionError as e:
            print(f"保存失败 (尝试 {attempt+1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                # 最后一次尝试，使用更简单的保存方式
                simple_path = output_path.with_suffix('.csv')
                print(f"尝试保存为CSV格式: {simple_path}")
                df.to_csv(simple_path, index=False, encoding='utf-8-sig')
                print(f"已将数据保存为CSV格式: {simple_path}")
            else:
                # 等待一点时间后重试
                time.sleep(2)
    
    print(f"共处理了 {file_count} 个文件的数据，总计 {final_count} 条有效记录")

if __name__ == "__main__":
    # 设置JSON文件目录路径
    json_directory = r"C:\Users\Admin\Desktop\discord-monitor-master0317\data\analysis_results"
    
    # 设置输出Excel文件路径
    output_excel = r"C:\Users\Admin\Desktop\discord-monitor-master0317\data\merged_results.xlsx"
    
    # 执行合并操作
    merge_json_files_to_excel(json_directory, output_excel) 