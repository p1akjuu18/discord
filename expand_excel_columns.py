import pandas as pd
import os
import json
import ast
import re
import traceback
from pathlib import Path

# 添加一个辅助函数来处理列表数据的展开
def process_list_items(data_list):
    """
    处理列表形式的数据，提取并整理每个项的内容
    
    参数:
    data_list: 包含字典或其他数据的列表
    
    返回:
    处理后的字典，其中键为item_0, item_1等
    """
    if not isinstance(data_list, list):
        return {"item_0": data_list}
    
    result = {}
    for i, item in enumerate(data_list):
        result[f"item_{i}"] = item
    return result

def expand_json_columns(file_path, columns_to_expand):
    """
    读取Excel文件，展开指定的列（可能是JSON或Python字典格式）
    
    参数:
    file_path: Excel文件路径
    columns_to_expand: 需要展开的列名列表
    """
    print(f"正在读取文件: {file_path}")
    
    # 读取Excel文件
    df = pd.read_excel(file_path)
    print(f"原始数据形状: {df.shape}, 列名: {', '.join(df.columns.tolist())}")
    
    # 对每一个需要展开的列进行处理
    for column in columns_to_expand:
        if column not in df.columns:
            print(f"警告: 列 '{column}' 不在数据中，跳过")
            continue
            
        print(f"\n--------- 正在展开列: {column} ---------")
        
        # 首先查看该列的前几个值，了解格式
        print(f"列 '{column}' 的前3个非空值示例:")
        non_empty_count = 0
        for i in range(min(20, len(df))):
            if i < len(df) and not pd.isna(df[column].iloc[i]) and str(df[column].iloc[i]).strip():
                print(f"  样本 {non_empty_count+1}: 类型={type(df[column].iloc[i])}, 值={df[column].iloc[i]}")
                non_empty_count += 1
                if non_empty_count >= 3:
                    break
        
        if non_empty_count == 0:
            print(f"  警告: 列 '{column}' 中没有找到非空值")
            continue
        
        # 尝试多种方式解析数据
        try:
            # 尝试将字符串转换为Python对象
            def safe_parse(x):
                if pd.isna(x):
                    return {}
                if not isinstance(x, str):
                    return {} if pd.isna(x) else x
                
                x = x.strip()
                if not x:
                    return {}
                
                # 处理NumPy类型的特殊情况
                # 替换 np.float64(123.45) 为 123.45
                x = re.sub(r'np\.float64\(([^)]+)\)', r'\1', x)
                # 替换 Timestamp('2024-07-22 06:35:00') 为 "2024-07-22 06:35:00"
                x = re.sub(r'Timestamp\(\'([^\']+)\'\)', r'"\1"', x)
                # 替换可能的其他NumPy类型
                x = re.sub(r'np\.[a-zA-Z0-9_]+\(([^)]+)\)', r'\1', x)
                
                # 如果字符串以[开头并以]结尾，可能是一个列表字符串
                if x.startswith('[') and x.endswith(']'):
                    try:
                        # 使用正则表达式修复列表中的字典
                        list_items = []
                        # 匹配列表中的每个字典
                        dict_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                        dict_matches = re.findall(dict_pattern, x)
                        
                        for dict_str in dict_matches:
                            fixed_dict_str = dict_str
                            # 替换单引号键为双引号键
                            fixed_dict_str = re.sub(r"'([^']+)':", r'"\1":', fixed_dict_str)
                            list_items.append(fixed_dict_str)
                        
                        if list_items:
                            fixed_list_str = "[" + ",".join(list_items) + "]"
                            try:
                                parsed_list = json.loads(fixed_list_str)
                                # 将列表转换为字典格式
                                return process_list_items(parsed_list)
                            except Exception:
                                pass
                    except Exception as e:
                        print(f"修复列表格式时出错: {str(e)}")
                
                # 尝试多种解析方法
                errors = []
                
                # 1. 直接尝试JSON解析
                try:
                    result = json.loads(x)
                    if isinstance(result, list):
                        return process_list_items(result)
                    return result
                except Exception as e:
                    errors.append(f"JSON解析失败: {str(e)}")
                
                # 2. 尝试修复常见的JSON格式问题再解析
                try:
                    # 修复单引号替换为双引号的问题
                    fixed_x = x.replace("'", "\"")
                    result = json.loads(fixed_x)
                    if isinstance(result, list):
                        return process_list_items(result)
                    return result
                except Exception as e:
                    errors.append(f"修复后JSON解析失败: {str(e)}")
                
                # 3. 尝试作为Python字典字符串解析
                try:
                    result = ast.literal_eval(x)
                    if isinstance(result, list):
                        return process_list_items(result)
                    return result
                except Exception as e:
                    errors.append(f"Python字典解析失败: {str(e)}")
                
                # 4. 尝试用正则表达式提取关键信息
                try:
                    # 针对特定格式的数据进行处理
                    # 这里需要根据实际数据格式定制正则表达式
                    pattern = r'\{(.*?)\}'  # 简单提取花括号中的内容
                    matches = re.findall(pattern, x)
                    if matches:
                        result = {}
                        for idx, match in enumerate(matches):
                            result[f"item_{idx}"] = match
                        return result
                except Exception as e:
                    errors.append(f"正则提取失败: {str(e)}")
                
                # 如果都失败了，输出详细的错误信息和值
                print(f"无法解析值: {x[:200]}...")
                print(f"解析错误: {', '.join(errors)}")
                return {}
            
            # 应用解析函数
            df[column] = df[column].apply(safe_parse)
            
            # 检查是否有成功解析的数据
            non_empty_dicts = df[column].apply(lambda x: bool(x)).sum()
            print(f"列 '{column}' 中成功解析的非空数据: {non_empty_dicts}/{len(df)}")
            
            if non_empty_dicts > 0:
                # 检查解析后的数据结构
                print("解析后的数据结构示例:")
                sample_dicts = []
                for i in range(len(df)):
                    if bool(df[column].iloc[i]):
                        sample_dicts.append(df[column].iloc[i])
                        print(f"  样本结构: {type(df[column].iloc[i])}, 键: {df[column].iloc[i].keys()}")
                        if len(sample_dicts) >= 2:
                            break
                
                # 展开该列
                try:
                    # 使用json_normalize扁平化嵌套的字典
                    expanded_df = pd.json_normalize(df[column].tolist())
                    
                    # 如果展开后有列
                    if not expanded_df.empty and expanded_df.shape[1] > 0:
                        print(f"展开后的列: {expanded_df.columns.tolist()}")
                        
                        # 为新列添加前缀，避免列名冲突
                        expanded_df = expanded_df.add_prefix(f"{column}_")
                        
                        # 删除原始列并将展开的列添加到数据框
                        df = df.drop(columns=[column])
                        df = pd.concat([df, expanded_df], axis=1)
                        
                        print(f"成功展开列: {column}，新增 {expanded_df.shape[1]} 列")
                    else:
                        print(f"列 '{column}' 展开后没有数据")
                except Exception as e:
                    print(f"展开数据时出错: {str(e)}")
                    traceback.print_exc()
            else:
                print(f"列 '{column}' 没有可解析的数据")
        except Exception as e:
            print(f"处理列 '{column}' 时出错: {str(e)}")
            traceback.print_exc()
    
    print(f"处理后数据形状: {df.shape}")
    return df

def main():
    # 获取桌面路径
    desktop = str(Path.home() / "Desktop")
    input_file = os.path.join(desktop, "result3_processed.xlsx")
    
    # 要展开的列
    columns_to_expand = ["entry_results", "entry_points_info", "total_profit_details"]
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 文件不存在 - {input_file}")
        return
    
    try:
        # 展开列
        expanded_df = expand_json_columns(input_file, columns_to_expand)
        
        # 保存结果
        output_file = os.path.join(desktop, "result3_expanded.xlsx")
        expanded_df.to_excel(output_file, index=False)
        print(f"已将展开后的数据保存至: {output_file}")
    except Exception as e:
        print(f"处理过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 