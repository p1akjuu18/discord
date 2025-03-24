import json
import os
from datetime import datetime
import requests
from typing import List, Dict, Optional, Tuple, Any, Union
from pathlib import Path
import pandas as pd
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import traceback
import re
import gc
import shutil
import subprocess
import sys
import uuid

# 添加关闭Excel连接的函数
def close_excel_connections():
    """尝试关闭所有Excel连接并释放文件锁"""
    try:
        if sys.platform == 'win32':
            # 尝试强制结束Excel进程
            subprocess.run(['taskkill', '/F', '/IM', 'EXCEL.EXE'], 
                           stdout=subprocess.DEVNULL, 
                           stderr=subprocess.DEVNULL, 
                           check=False)
            # 强制垃圾回收
            gc.collect()
            # 等待系统释放文件
            time.sleep(1)
    except Exception as e:
        print(f"关闭Excel连接时出错: {e}")

def get_output_dir():
    """获取统一的输出目录"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "data", "analysis_results")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

class MessageFileHandler(FileSystemEventHandler):
    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.processed_files = set()
        print("消息处理器已初始化")  # 添加初始化提示
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            print(f"\n检测到新文件: {event.src_path}")  # 添加文件检测提示
            self.process_file(event.src_path)
            
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            print(f"\n检测到文件修改: {event.src_path}")  # 添加文件修改提示
            self.process_file(event.src_path)
    
    def process_file(self, file_path):
        try:
            # 跳过土狗博主群ca.json文件
            if "1283359910788202499-土狗博主群ca.json" in file_path:
                print(f"\n跳过土狗博主群ca.json文件: {file_path}")
                return
                
            # 确保文件写入完成
            time.sleep(1)
            
            print(f"\n检测到新文件: {file_path}")
            print(f"开始处理文件: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print("成功读取JSON文件")
            
            # 从文件名中提取频道名称
            channel_name = os.path.basename(file_path).split('-')[1].replace('.json', '')
            print(f"处理频道: {channel_name}")
            
            # 判断数据结构类型并获取消息
            messages = data if isinstance(data, list) else data.get('messages', [])
            
            if messages:
                latest_message = messages[-1]  # 获取最后一条消息
                content = latest_message.get('content', '')
                print(f"\n最新消息内容: {content}")  # 完整打印消息内容
                print("\n开始调用 DeepSeek API 进行分析...")
                
                # 调用 DeepSeek API 分析消息
                result = self.analyzer.analyze_message(content, channel_name)
                if result:
                    print("\nDeepSeek API 分析成功!")
                    print("分析结果:")
                    print(json.dumps(result, ensure_ascii=False, indent=2))
                    
                    # 添加元数据
                    enriched_result = {
                        'channel': channel_name,
                        'timestamp': latest_message.get('timestamp'),
                        'message_id': latest_message.get('id'),
                        'author': latest_message.get('author'),
                        'author_id': latest_message.get('author_id'),
                        'attachments': latest_message.get('attachments', []),
                        'analysis': result
                    }
                    
                    # 保存结果
                    output_dir = get_output_dir()
                    
                    # 保存到频道特定的JSON文件
                    channel_file = os.path.join(output_dir, f"{channel_name}_results.json")
                    channel_results = []
                    
                    # 如果文件已存在，读取现有结果
                    if os.path.exists(channel_file):
                        with open(channel_file, 'r', encoding='utf-8') as f:
                            channel_results = json.load(f)
                    
                    # 添加新结果
                    channel_results.append(enriched_result)
                    
                    # 保存更新后的结果
                    with open(channel_file, 'w', encoding='utf-8') as f:
                        json.dump(channel_results, f, ensure_ascii=False, indent=2)
                    
                    print(f"\n结果已保存到: {channel_file}")
                    
                    # 关键改动：确保单条消息分析结果被正确添加到Excel中
                    try:
                        # 尝试先关闭可能存在的Excel连接
                        close_excel_connections()
                        
                        excel_path = process_single_message(enriched_result, output_dir)
                        if excel_path:
                            print(f"\n已成功更新Excel文件: {excel_path}")
                        else:
                            print("\n警告：更新Excel文件失败")
                        
                        # 收集所有JSON结果并尝试更新主Excel文件
                        all_results = []
                        for filename in os.listdir(output_dir):
                            if filename.endswith('_results.json'):
                                json_path = os.path.join(output_dir, filename)
                                try:
                                    with open(json_path, 'r', encoding='utf-8') as f:
                                        channel_data = json.load(f)
                                        all_results.extend(channel_data)
                                except Exception as e:
                                    print(f"读取{json_path}时出错: {e}")
                        
                        if all_results:
                            # 尝试重新生成完整的Excel文件
                            excel_path = process_analysis_data(all_results, output_dir)
                            if excel_path:
                                print(f"\n已成功重建完整Excel文件: {excel_path}")
                            else:
                                print("\n警告：重建Excel文件失败")
                        
                    except Exception as e:
                        print(f"\nExcel处理时出错: {str(e)}")
                        traceback.print_exc()  # 打印详细错误堆栈
                    
                else:
                    print("\nDeepSeek API 分析失败或返回空结果")
            else:
                print("文件中没有找到消息数据")
            
            print(f"\n文件处理完成: {file_path}")
            
        except json.JSONDecodeError as e:
            print(f"JSON解析错误 {file_path}: {str(e)}")
        except Exception as e:
            print(f"处理文件时出错 {file_path}: {str(e)}")
            traceback.print_exc()  # 打印详细错误堆栈

class HistoricalMessageAnalyzer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # 默认分析提示词
        self.default_prompt = """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
"""
        
        # 针对不同博主的自定义提示词
        self.channel_prompts = {
            "交易员张张子": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，一般会提到大饼=BTC=$btc，以太=ETH=$eth,SOL,BNB,DOGE。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。多单多以"支撑位"为入场点位，空单多以"压力位"为入场点位。会提到"留意"的位置。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。多单多以"压力位"为止盈点位。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对各个不同币种的市场分析和走势预测，每个币种单独记录。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "打不死的交易员": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",


            "tia-初塔": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "舒琴实盘": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "三马合约": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "三马现货": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。其中，如果没有指代任何货币，并且描述会是以70k-100k为单位和价格，那么，这个币种是BTC。需要把k转换成80000-100000这样的描述。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "btc欧阳": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "加密大漂亮": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "大漂亮会员策略": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "shu-crypto": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "三木的交易日记": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "大镖客比特币行情": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "trader-titan": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "traeep": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "john": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "Michelle": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "eliz": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "hbj": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "woods": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "Dr profit": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。其中，如果没有指代任何货币，并且描述会是以70k-100k为单位和价格，那么，这个币种是BTC。需要把k转换成80000-100000这样的描述。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
""",

            "Rose": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 入场点位1：第一个入场价格
   - 入场点位2：第二个入场价格（如果有）
   - 入场点位3：第三个入场价格（如果有）

5. **止损点位**：如果博主提到止损价格，请列出。
   - 止损点位1：第一个止损价格
   - 止损点位2：第二个止损价格（如果有）
   - 止损点位3：第三个止损价格（如果有）

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 止盈点位1：第一个止盈价格
   - 止盈点位2：第二个止盈价格（如果有）
   - 止盈点位3：第三个止盈价格（如果有）

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，格式如下：
{{
    "交易币种": "币种名称",
    "方向": "交易方向",
    "杠杆": 杠杆倍数或null,
    "入场点位1": 数字或null,
    "入场点位2": 数字或null,
    "入场点位3": 数字或null,
    "止损点位1": 数字或null,
    "止损点位2": 数字或null,
    "止损点位3": 数字或null,
    "止盈点位1": 数字或null,
    "止盈点位2": 数字或null,
    "止盈点位3": 数字或null,
    "分析内容": "分析文字"
}}

价格必须是数字（不含单位），未提及的信息用null。
"""
        }


        # 默认的消息筛选规则
        self.default_filter = {
            "min_length": 10,
            "price_indicators": ['$', '美元', 'k', 'K', '千', '万'],
            "trading_keywords": ['多', '空', '做多', '做空', '买入', '卖出', '止损', '止盈', 
                               'long', 'short', 'buy', 'sell', 'stop', 'target'],
            "required_keywords": [],
            "excluded_keywords": []
        }
        
        # 添加这个：各频道的特定筛选规则
        self.channel_filters = {
            # 如果某个频道需要特殊的筛选规则，可以在这里添加
            # 例如：
            # "channel_name": { ... }
        }

    def _extract_translated_content(self, content: str) -> Tuple[str, str]:
        """提取原文和翻译内容"""
        try:
            if "**原文:**" in content and "**翻译:**" in content:
                parts = content.split("**翻译:**")
                if len(parts) >= 2:
                    original = parts[0].replace("**原文:**", "").strip()
                    translated = parts[1].split("--------------")[0].strip()
                    return original, translated
            return content, content
        except Exception as e:
            print(f"提取翻译内容时出错: {str(e)}")
            return content, content

    def should_analyze_message(self, msg: Dict, channel_name: str = None) -> bool:
        """判断消息是否需要分析"""
        if not msg.get('content'):
            return False
            
        content = msg['content']
        
        # 获取对应频道的筛选规则，如果没有则使用默认规则
        filter_rules = self.channel_filters.get(channel_name, self.default_filter)
        
        # 检查消息长度
        if len(content.strip()) < filter_rules["min_length"]:
            return False
            
        # 检查是否包含需要排除的关键词
        if any(keyword in content.lower() for keyword in filter_rules["excluded_keywords"]):
            return False
            
        # 检查是否包含必需的关键词（如果有设置）
        if filter_rules["required_keywords"] and not any(keyword in content for keyword in filter_rules["required_keywords"]):
            return False
            
        # 检查是否包含价格相关信息
        has_price = any(indicator in content for indicator in filter_rules["price_indicators"])
        
        # 检查是否包含交易相关词汇
        has_trading_terms = any(keyword in content.lower() for keyword in filter_rules["trading_keywords"])
        
        return has_price or has_trading_terms

    def preprocess_message(self, content: str) -> str:
        """消息预处理，增强可分析性"""
        if not content:
            return content
            
        # 去除多余的换行和空格
        content = re.sub(r'\n+', '\n', content)
        content = re.sub(r' +', ' ', content)
        
        # 标准化常见的价格表示方式
        content = re.sub(r'(\d+)([kK])', r'\1000', content)  # 将10k转换为10000
        content = re.sub(r'(\d+)\.(\d+)([kK])', r'\1\200', content)  # 将1.5k转换为1500
        
        # 标准化币种名称
        currency_pairs = {
            '大饼': 'BTC',
            '比特币': 'BTC',
            '以太坊': 'ETH',
            '二饼': 'ETH',
        }
        
        for original, replacement in currency_pairs.items():
            content = content.replace(original, replacement)
        
        # 标准化方向词汇
        direction_pairs = {
            '看涨': '做多',
            '看跌': '做空',
            '买入': '做多',
            '卖出': '做空',
        }
        
        for original, replacement in direction_pairs.items():
            content = content.replace(original, replacement)
        
        return content

    def analyze_message(self, content: str, channel_name: str = None, retry_count: int = 3) -> Optional[Dict]:
        """分析单条消息"""
        original, translated = self._extract_translated_content(content)
        
        # 使用翻译内容进行分析
        content_to_analyze = translated or original
        
        # 预处理消息内容
        content_to_analyze = self.preprocess_message(content_to_analyze)
        
        if not content_to_analyze or len(content_to_analyze.strip()) < 10:
            print(f"消息内容太短或为空，跳过分析: {content[:30]}...")
            return None
        
        # 首先尝试使用正则表达式提取基本信息
        extracted_info = self._extract_basic_trading_info(content_to_analyze)
        print(f"预提取信息结果: {extracted_info}")
        
        # 选择对应的提示词
        prompt = self.channel_prompts.get(channel_name, self.default_prompt)
        
        # 增强提示词，加入预提取信息
        enhanced_prompt = self._enhance_prompt_with_extracted_info(prompt, extracted_info)
        
        messages = [{"role": "user", "content": enhanced_prompt.format(content=content_to_analyze)}]
        
        for attempt in range(retry_count):
            try:
                print(f"正在使用{channel_name if channel_name else '默认'}提示词分析消息: {content_to_analyze[:100]}...")
                
                # API 调用逻辑保持不变
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=self.headers,
                    json={
                        "model": "deepseek-ai/DeepSeek-V3",
                        "messages": messages,
                        "max_tokens": 1024,
                        "temperature": 0.7
                    },
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                
                if 'choices' in result and len(result['choices']) > 0:
                    content = result['choices'][0]['message']['content']
                    try:
                        # 清理返回的内容，移除markdown标记
                        cleaned_content = content.replace('```json', '').replace('```', '').strip()
                        parsed_result = json.loads(cleaned_content)
                        print("分析成功！")
                        
                        # 合并预提取结果和API分析结果
                        merged_result = self._merge_analysis_results(extracted_info, parsed_result)
                        
                        # 添加原文和翻译到结果中
                        merged_result['原文'] = original
                        merged_result['翻译'] = translated
                        return merged_result
                    except json.JSONDecodeError as e:
                        print(f"JSON解析失败: {content}")
                        print(f"错误详情: {str(e)}")
                        
                        # 如果API分析失败，但预提取成功，则返回预提取结果
                        if any(extracted_info.values()):
                            print("API分析失败，使用预提取结果作为备选")
                            extracted_info['原文'] = original
                            extracted_info['翻译'] = translated
                            extracted_info['分析内容'] = "通过规则提取的基本信息，API分析失败"
                            return extracted_info
                        
                        return None
                else:
                    print(f"API返回结果没有有效内容: {result}")
                    # 如果API返回为空但预提取成功，则返回预提取结果
                    if any(extracted_info.values()):
                        print("API分析返回空结果，使用预提取结果作为备选")
                        extracted_info['原文'] = original
                        extracted_info['翻译'] = translated
                        extracted_info['分析内容'] = "通过规则提取的基本信息，API分析未返回结果"
                        return extracted_info
                    
            except requests.exceptions.RequestException as e:
                print(f"API请求失败 (尝试 {attempt + 1}/{retry_count}): {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                print(f"未知错误 (尝试 {attempt + 1}/{retry_count}): {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
        
        # 如果API多次尝试都失败，但预提取成功，则返回预提取结果
        if any(extracted_info.values()):
            print("经过多次尝试API分析仍失败，使用预提取结果作为备选")
            extracted_info['原文'] = original
            extracted_info['翻译'] = translated
            extracted_info['分析内容'] = "通过规则提取的基本信息，API分析多次失败"
            return extracted_info
        
        return None

    def _extract_basic_trading_info(self, content: str) -> Dict:
        """使用规则和正则表达式从消息内容中提取基本交易信息"""
        result = {
            "交易币种": None,
            "方向": None,
            "杠杆": None,
            "入场点位1": None,
            "入场点位2": None,
            "入场点位3": None,
            "止损点位1": None,
            "止损点位2": None,
            "止损点位3": None,
            "止盈点位1": None,
            "止盈点位2": None,
            "止盈点位3": None,
            "分析内容": None
        }
        
        # 尝试提取币种
        currency_pattern = r'\b(BTC|ETH|SOL|DOGE|XRP|BNB|ADA|DOT|TRX|AVAX|LINK|LTC|BCH|EOS)\b'
        currency_match = re.search(currency_pattern, content, re.IGNORECASE)
        if currency_match:
            result["交易币种"] = currency_match.group(1).upper()
        
        # 尝试提取交易方向
        if re.search(r'\b(做多|多头|多单|看涨|bull|buy|long)\b', content, re.IGNORECASE):
            result["方向"] = "做多"
        elif re.search(r'\b(做空|空头|空单|看跌|bear|sell|short)\b', content, re.IGNORECASE):
            result["方向"] = "做空"
        
        # 尝试提取杠杆
        leverage_pattern = r'(\d+)[xX倍]杠杆'
        leverage_match = re.search(leverage_pattern, content)
        if leverage_match:
            result["杠杆"] = int(leverage_match.group(1))
        
        # 尝试提取入场点位
        entry_patterns = [
            r'入场[价位点]?[：:]*\s*([\d\.]+)',
            r'进场[价位点]?[：:]*\s*([\d\.]+)',
            r'[买卖][入出]点?[：:]*\s*([\d\.]+)'
        ]
        
        entry_positions = []
        for pattern in entry_patterns:
            for match in re.finditer(pattern, content):
                entry_positions.append(float(match.group(1)))
        
        # 填充入场点位
        for i, pos in enumerate(entry_positions[:3], 1):
            result[f"入场点位{i}"] = pos
        
        # 尝试提取止损点位
        sl_patterns = [
            r'止损[价位点]?[：:]*\s*([\d\.]+)',
            r'SL[：:]*\s*([\d\.]+)',
            r'sl[：:]*\s*([\d\.]+)'
        ]
        
        sl_positions = []
        for pattern in sl_patterns:
            for match in re.finditer(pattern, content):
                sl_positions.append(float(match.group(1)))
        
        # 填充止损点位
        for i, pos in enumerate(sl_positions[:3], 1):
            result[f"止损点位{i}"] = pos
        
        # 尝试提取止盈点位
        tp_patterns = [
            r'止盈[价位点]?[：:]*\s*([\d\.]+)',
            r'目标[价位点]?[：:]*\s*([\d\.]+)',
            r'TP[：:]*\s*([\d\.]+)',
            r'tp[：:]*\s*([\d\.]+)'
        ]
        
        tp_positions = []
        for pattern in tp_patterns:
            for match in re.finditer(pattern, content):
                tp_positions.append(float(match.group(1)))
        
        # 填充止盈点位
        for i, pos in enumerate(tp_positions[:3], 1):
            result[f"止盈点位{i}"] = pos
        
        return result

    def _enhance_prompt_with_extracted_info(self, prompt: str, extracted_info: Dict) -> str:
        """根据预提取的信息增强提示词"""
        # 如果没有提取到任何信息，直接返回原提示词
        if not any(extracted_info.values()):
            return prompt
        
        # 添加一段提示，告诉API我们已经预提取了一些信息，你可以参考这些信息进行更准确的分析：
        enhancement = "\n以下是通过简单规则预先提取的信息，你可以参考这些信息进行更准确的分析：\n"
        
        for key, value in extracted_info.items():
            if value is not None:
                enhancement += f"{key}: {value}\n"
        
        # 在原提示词的适当位置插入增强内容
        enhanced_prompt = prompt.replace("内容如下：", f"{enhancement}\n内容如下：")
        
        return enhanced_prompt

    def _merge_analysis_results(self, extracted_info: Dict, api_result: Dict) -> Dict:
        """合并预提取结果和API分析结果"""
        merged_result = api_result.copy()
        
        # 对于API没有分析出来但预提取有的字段，使用预提取的结果
        for key, value in extracted_info.items():
            if value is not None and (key not in merged_result or merged_result[key] is None):
                merged_result[key] = value
        
        return merged_result

    def _log_api_interaction(self, messages, response, channel_name):
        """记录API请求和响应到日志文件"""
        try:
            log_dir = "data/analysis_logs"
            os.makedirs(log_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"api_log_{timestamp}_{channel_name}.json")
            
            log_data = {
                "timestamp": timestamp,
                "channel": channel_name,
                "request": {
                    "messages": messages
                },
                "response": {
                    "status_code": response.status_code,
                    "content": response.text
                }
            }
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"记录API交互时出错: {str(e)}")

    def _log_parse_error(self, content, channel_name, error_msg):
        """记录解析错误到日志文件"""
        try:
            log_dir = "data/analysis_logs/parse_errors"
            os.makedirs(log_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"parse_error_{timestamp}_{channel_name}.txt")
            
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"时间: {timestamp}\n")
                f.write(f"频道: {channel_name}\n")
                f.write(f"错误: {error_msg}\n\n")
                f.write("原始内容:\n")
                f.write(content)
                
        except Exception as e:
            print(f"记录解析错误时出错: {str(e)}")

    def _try_fallback_parsing(self, content, original, translated):
        """尝试使用更宽松的方式解析JSON"""
        print("尝试备选解析方法...")
        try:
            # 尝试寻找JSON结构的开始和结束位置
            start_pos = content.find('{')
            end_pos = content.rfind('}') + 1
            
            if start_pos >= 0 and end_pos > start_pos:
                json_content = content[start_pos:end_pos]
                parsed_result = json.loads(json_content)
                print("备选解析成功！")
                parsed_result['原文'] = original
                parsed_result['翻译'] = translated
                return parsed_result
        except Exception as e:
            print(f"备选解析也失败了: {str(e)}")
        
        return None

    def _create_empty_result(self, original, translated, channel_name, content):
        """创建一个带有基本结构的空结果"""
        print("创建一个基本的空结果")
        
        # 尝试从内容中提取可能的交易币种
        possible_currency = self._extract_possible_currency(content)
        
        empty_result = {
            "交易币种": possible_currency,
            "方向": None,
            "杠杆": None,
            "入场点位1": None,
            "入场点位2": None,
            "入场点位3": None,
            "止损点位1": None,
            "止损点位2": None,
            "止损点位3": None,
            "止盈点位1": None,
            "止盈点位2": None,
            "止盈点位3": None,
            "分析内容": "分析失败，未能提取有效信息",
            "原文": original,
            "翻译": translated,
            "分析失败": True
        }
        
        # 记录失败案例以供后续改进
        self._log_analysis_failure(content, channel_name, empty_result)
        
        return empty_result

    def _extract_possible_currency(self, content):
        """从内容中尝试提取可能的交易币种"""
        # 常见币种列表
        common_currencies = ["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "ADA", "DOT", "TRX", "AVAX"]
        
        for currency in common_currencies:
            if currency in content:
                return currency
        
        return None

    def _log_analysis_failure(self, content, channel_name, empty_result):
        """记录分析失败案例"""
        try:
            log_dir = "data/analysis_logs/failures"
            os.makedirs(log_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"analysis_failure_{timestamp}_{channel_name}.json")
            
            log_data = {
                "timestamp": timestamp,
                "channel": channel_name,
                "content": content,
                "empty_result": empty_result
            }
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"记录分析失败时出错: {str(e)}")

    def process_message_files(self, data_dir: str, output_dir: str):
        """处理所有消息文件"""
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取所有JSON文件
        json_files = list(Path(data_dir).glob("*.json"))
        total_files = len(json_files)
        
        if not json_files:
            print(f"警告：在目录 {data_dir} 中没有找到JSON文件")
            return
        
        all_results = []
        processed_messages = 0
        skipped_messages = 0
        
        # 创建时间戳，用于文件命名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(output_dir, f"analysis_results_{timestamp}.json")
        excel_file = os.path.join(output_dir, f"analysis_data_{timestamp}.xlsx")
        
        # 用于存储每个频道的结果
        channel_results = {}
        
        for i, file_path in enumerate(json_files, 1):
            print(f"\n处理文件 {i}/{total_files}: {file_path.name}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                channel_name = self._extract_channel_name(file_path.name)
                print(f"频道名称: {channel_name}")
                
                if isinstance(data, list):  # 如果文件内容直接是消息数组
                    messages = data
                elif isinstance(data, dict) and 'messages' in data:  # 如果消息在messages字段中
                    messages = data['messages']
                else:
                    print(f"警告：文件 {file_path.name} 格式不正确")
                    continue
                    
                print(f"发现 {len(messages)} 条消息")
                
                # 确保该频道在字典中有一个列表
                if channel_name not in channel_results:
                    channel_results[channel_name] = []
                
                for j, msg in enumerate(messages, 1):
                    processed_messages += 1
                    
                    if not self.should_analyze_message(msg, channel_name):
                        skipped_messages += 1
                        print(f"跳过消息 {j}: 不符合分析条件")
                        continue
                    
                    print(f"\n处理消息 {j}/{len(messages)}")
                    result = self.analyze_message(msg.get('content', ''), channel_name)
                    
                    if result:
                        # 添加元数据
                        enriched_result = {
                            'channel': channel_name,
                            'timestamp': msg.get('timestamp'),
                            'message_id': msg.get('id'),
                            'author': msg.get('author'),
                            'author_id': msg.get('author_id'),
                            'attachments': msg.get('attachments', []),
                            'analysis': result
                        }
                        channel_results[channel_name].append(enriched_result)
                        all_results.append(enriched_result)
                        
                        # 每处理完一条消息就更新该频道的文件
                        self._save_channel_results(channel_results, output_dir)
                    
                print(f"文件 {file_path.name} 分析完成，成功分析 {len(channel_results[channel_name])} 条消息")
                
            except Exception as e:
                print(f"处理文件时出错 {file_path}: {str(e)}")
            
        print(f"\n处理完成:")
        print(f"处理了 {total_files} 个文件")
        print(f"处理了 {processed_messages} 条消息")
        print(f"跳过了 {skipped_messages} 条消息")
        print(f"成功分析了 {len(all_results)} 条消息")
        
        # 最终生成统计报告
        if all_results:
            self._generate_report(all_results, output_dir)
        else:
            print("警告：没有成功分析任何消息")

    def _extract_channel_name(self, filename: str) -> str:
        """从文件名提取频道名称"""
        parts = filename.split('-')
        if len(parts) >= 2:
            return '-'.join(parts[1:]).replace('.json', '')
        return filename.replace('.json', '')

    def _save_channel_results(self, channel_results: Dict[str, List[Dict]], output_dir: str):
        """保存每个频道的分析结果"""
        try:
            # 保存每个频道的结果到对应的JSON文件
            for channel_name, results in channel_results.items():
                channel_file = os.path.join(output_dir, f"{channel_name}_results.json")
                with open(channel_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                
            # 保存所有结果到Excel文件 - 替换为高级数据处理
            all_results = []
            for results in channel_results.values():
                all_results.extend(results)
                
            if all_results:
                excel_path = process_analysis_data(all_results, output_dir)
                print(f"\n已更新Excel文件: {excel_path}")
                
        except Exception as e:
            print(f"保存频道结果时出错: {str(e)}")

    def _generate_report(self, results: List[Dict], output_dir: str):
        """生成分析报告"""
        if not results:
            print("警告：没有分析结果可供生成报告")
            report = {
                "总消息数": 0,
                "频道统计": {},
                "每日消息数": {},
                "币种统计": {},
                "交易方向统计": {}
            }
        else:
            # 转换为DataFrame
            df = pd.json_normalize(results)
            
            # 处理可能的列表类型值
            def safe_value_counts(series):
                # 将列表类型的值转换为字符串
                processed_series = series.apply(lambda x: str(x) if isinstance(x, list) else x)
                return processed_series.value_counts().to_dict()
            
            # 基础统计
            report = {
                "总消息数": len(results),
                "频道统计": safe_value_counts(df['channel']) if 'channel' in df.columns else {},
                "每日消息数": df['timestamp'].str[:10].value_counts().to_dict() if 'timestamp' in df.columns else {},
                "币种统计": safe_value_counts(df['analysis.交易币种']) if 'analysis.交易币种' in df.columns else {},
                "交易方向统计": safe_value_counts(df['analysis.方向']) if 'analysis.方向' in df.columns else {}
            }
            
            # 添加更详细的统计信息
            try:
                # 计算每个频道的消息数量趋势
                if 'timestamp' in df.columns and 'channel' in df.columns:
                    df['date'] = pd.to_datetime(df['timestamp']).dt.date
                    channel_trends = df.groupby(['channel', 'date']).size().to_dict()
                    report["频道消息趋势"] = {str(k): v for k, v in channel_trends.items()}
                
                # 计算交易方向的比例
                if 'analysis.方向' in df.columns:
                    direction_total = len(df['analysis.方向'].dropna())
                    direction_counts = safe_value_counts(df['analysis.方向'])
                    report["交易方向比例"] = {
                        k: f"{(v/direction_total*100):.2f}%" 
                        for k, v in direction_counts.items()
                    }
                
            except Exception as e:
                print(f"生成详细统计信息时出错: {str(e)}")
        
        # 保存统计报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(output_dir, f"analysis_report_{timestamp}.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n分析报告已生成：{report_file}")
        
        # 打印主要统计信息
        print("\n=== 统计摘要 ===")
        print(f"总消息数: {report['总消息数']}")
        print("\n频道统计:")
        for channel, count in report['频道统计'].items():
            print(f"  {channel}: {count}条消息")
        if "交易方向比例" in report:
            print("\n交易方向比例:")
            for direction, percentage in report['交易方向比例'].items():
                print(f"  {direction}: {percentage}")

    def start_monitoring(self, path: str):
        """开始监控文件夹"""
        event_handler = MessageFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, path, recursive=False)
        observer.start()
        
        try:
            print(f"开始监控文件夹: {path}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            print("监控已停止")
        
        observer.join()

def standardize_direction(value):
    """标准化交易方向：统一多头和空头的表示"""
    if pd.isna(value) or value == '':
        return value
    
    value_lower = str(value).lower()  # 转为小写便于比较
    
    # 统一多头表示
    long_terms = ['做多', '多单', '多', 'long', 'buy', '买入']
    for term in long_terms:
        if term.lower() in value_lower:
            return '做多'
    
    # 统一空头表示
    short_terms = ['做空', '空单', '空', 'short', 'sell', '卖出']
    for term in short_terms:
        if term.lower() in value_lower:
            return '做空'
    
    return value  # 如果没有匹配，保持原值

def clean_currency(value):
    """标准化币种名称"""
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

def clean_position_value(value):
    """清理并标准化点位数据"""
    if pd.isna(value) or value == '':
        return None
        
    # 如果是字符串类型，尝试清理
    if isinstance(value, str):
        # 去除方括号、空格和其他非数字字符
        cleaned = value.strip('[]').strip()
        if cleaned == '':
            return None
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None
            
    # 尝试转换为浮点数
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def process_analysis_data(data_list, output_dir="data/analysis_results"):
    """处理分析数据并保存到CSV"""
    if not data_list:
        print("没有数据需要处理")
        return None
    
    print(f"处理{len(data_list)}条分析记录...")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 转换为DataFrame
    df = pd.json_normalize(data_list)
    
    # 修改标准化数据部分
    if 'analysis.方向' in df.columns:
        df['analysis.方向'] = df['analysis.方向'].apply(standardize_direction)
        
    if 'analysis.交易币种' in df.columns:
        df['analysis.交易币种'] = df['analysis.交易币种'].apply(clean_currency)
    
    # 处理点位数据
    for col in df.columns:
        if any(keyword in col for keyword in ['入场点位', '止盈点位', '止损点位']):
            df[col] = df[col].apply(clean_position_value)
    
    # 处理列表类型
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x)
    
    # 生成唯一的临时文件名避免文件锁定问题
    temp_path = os.path.join(output_dir, f"temp_{uuid.uuid4().hex}.csv")
    output_path = os.path.join(output_dir, "all_analysis_results.csv")
    
    try:
        print(f"保存到临时文件: {temp_path}")
        # 保存到临时CSV文件
        df.to_csv(temp_path, index=False, encoding='utf-8-sig')
        
        # 如果临时文件保存成功，替换原文件
        if os.path.exists(temp_path):
            # 先关闭所有可能的文件连接
            close_excel_connections()
            time.sleep(1)  # 等待文件释放
            
            # 如果原文件存在，先尝试备份
            if os.path.exists(output_path):
                backup_path = output_path + ".bak"
                try:
                    shutil.copy2(output_path, backup_path)
                    print(f"已创建原文件备份: {backup_path}")
                except Exception as backup_err:
                    print(f"创建备份文件失败: {backup_err}")
            
            # 替换原文件
            try:
                shutil.move(temp_path, output_path)
                print(f"成功更新CSV文件: {output_path}")
            except Exception as move_err:
                print(f"移动临时文件失败: {move_err}")
                print("尝试直接写入原文件...")
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
        else:
            print(f"错误：临时文件 {temp_path} 未成功创建")
            # 直接尝试写入原文件
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # 检查CSV文件是否已成功保存
        if os.path.exists(output_path):
            print(f"确认CSV文件已保存: {output_path}")
            
            # 尝试创建Excel版本
            excel_path = output_path.replace('.csv', '.xlsx')
            try:
                print(f"尝试创建Excel版本: {excel_path}")
                # 添加错误处理和重试逻辑
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        # 先确保关闭任何可能的Excel连接
                        close_excel_connections()
                        
                        # 读取保存好的CSV，再转为Excel
                        pd.read_csv(output_path, encoding='utf-8-sig').to_excel(excel_path, index=False, engine='openpyxl')
                        print(f"成功创建Excel版本: {excel_path}")
                        break
                    except Exception as retry_err:
                        print(f"第{retry+1}次尝试创建Excel失败: {retry_err}")
                        if retry == max_retries - 1:
                            print("达到最大重试次数，无法创建Excel版本，但CSV文件已正常保存")
                        else:
                            time.sleep(1)  # 等待一秒后重试
            except Exception as excel_e:
                print(f"创建Excel版本失败，但CSV文件已正常保存: {excel_e}")
                traceback.print_exc()
                
            return output_path
        else:
            print(f"警告：CSV文件{output_path}不存在，保存可能失败")
            return None
            
    except Exception as e:
        print(f"保存CSV文件失败: {e}")
        traceback.print_exc()
        return None

def process_single_message(message_data, output_dir="data/analysis_results"):
    """处理单条消息并更新CSV文件"""
    print("\n=== 处理单条消息 ===")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 固定的输出文件路径
    output_path = os.path.join(output_dir, "all_analysis_results.csv")
    
    # 生成唯一的临时文件名避免文件锁定问题
    temp_path = os.path.join(output_dir, f"temp_{uuid.uuid4().hex}.csv")
    
    # 准备新数据
    df_new = pd.json_normalize([message_data])
    
    # 标准化数据处理
    if 'analysis.方向' in df_new.columns:
        df_new['analysis.方向'] = df_new['analysis.方向'].apply(standardize_direction)
        
    if 'analysis.交易币种' in df_new.columns:
        df_new['analysis.交易币种'] = df_new['analysis.交易币种'].apply(clean_currency)
    
    # 处理点位数据
    for col in df_new.columns:
        if any(keyword in col for keyword in ['入场点位', '止盈点位', '止损点位']):
            df_new[col] = df_new[col].apply(clean_position_value)
    
    # 处理列表类型
    for col in df_new.columns:
        df_new[col] = df_new[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x)
    
    try:
        print(f"准备保存数据到: {output_path}")
        
        # 如果文件存在，读取并追加数据
        if os.path.exists(output_path):
            try:
                print("检测到现有CSV文件，尝试读取...")
                # 读取现有CSV文件
                existing_df = pd.read_csv(output_path, encoding='utf-8-sig')
                print(f"成功读取现有CSV文件，包含{len(existing_df)}条记录")
                
                # 合并数据
                combined_df = pd.concat([existing_df, df_new], ignore_index=True)
                print(f"合并后数据包含{len(combined_df)}条记录")
                
                # 去重
                if 'message_id' in combined_df.columns:
                    old_len = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=['message_id'])
                    print(f"去重后数据包含{len(combined_df)}条记录(移除了{old_len-len(combined_df)}条)")
                
                # 先保存到临时文件
                print(f"保存到临时文件: {temp_path}")
                combined_df.to_csv(temp_path, index=False, encoding='utf-8-sig')
                
                # 如果临时文件保存成功，替换原文件
                if os.path.exists(temp_path):
                    # 先关闭所有可能的文件连接
                    close_excel_connections()
                    time.sleep(1)  # 等待文件释放
                    
                    # 如果原文件存在，先尝试备份
                    if os.path.exists(output_path):
                        backup_path = output_path + ".bak"
                        try:
                            shutil.copy2(output_path, backup_path)
                            print(f"已创建原文件备份: {backup_path}")
                        except Exception as backup_err:
                            print(f"创建备份文件失败: {backup_err}")
                    
                    # 替换原文件
                    try:
                        shutil.move(temp_path, output_path)
                        print(f"成功更新CSV文件: {output_path}")
                    except Exception as move_err:
                        print(f"移动临时文件失败: {move_err}")
                        print("尝试直接写入原文件...")
                        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                else:
                    print(f"错误：临时文件 {temp_path} 未成功创建")
                    # 直接尝试写入原文件
                    combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                    
            except Exception as e:
                print(f"处理现有CSV文件时出错: {e}")
                traceback.print_exc()
                print("尝试直接创建新文件...")
                df_new.to_csv(output_path, index=False, encoding='utf-8-sig')
        else:
            # 如果文件不存在，直接创建新文件
            print("CSV文件不存在，创建新文件...")
            df_new.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"成功创建新CSV文件: {output_path}")
        
        # 检查CSV文件是否已成功保存
        if os.path.exists(output_path):
            print(f"确认CSV文件已保存: {output_path}")
            
            # 为了方便用户，也可以再导出一份Excel版本
            excel_path = output_path.replace('.csv', '.xlsx')
            try:
                print(f"尝试创建Excel版本: {excel_path}")
                # 添加错误处理和重试逻辑
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        # 先确保关闭任何可能的Excel连接
                        close_excel_connections()
                        
                        # 读取保存好的CSV，再转为Excel
                        pd.read_csv(output_path, encoding='utf-8-sig').to_excel(excel_path, index=False, engine='openpyxl')
                        print(f"成功创建Excel版本: {excel_path}")
                        break
                    except Exception as retry_err:
                        print(f"第{retry+1}次尝试创建Excel失败: {retry_err}")
                        if retry == max_retries - 1:
                            print("达到最大重试次数，无法创建Excel版本，但CSV文件已正常保存")
                        else:
                            time.sleep(1)  # 等待一秒后重试
            except Exception as excel_e:
                print(f"创建Excel版本失败，但CSV文件已正常保存: {excel_e}")
                traceback.print_exc()
        else:
            print(f"警告：CSV文件{output_path}不存在，保存可能失败")
            
        return output_path
        
    except Exception as e:
        print(f"保存CSV文件失败: {e}")
        traceback.print_exc()
        return None

def safe_save_file(df, output_path, max_retries=3):
    """安全保存文件，包含重试机制"""
    for attempt in range(max_retries):
        try:
            # 确保关闭所有可能的文件连接
            close_excel_connections()
            time.sleep(1)  # 等待文件释放
            
            # 使用临时文件
            temp_path = output_path.with_suffix('.tmp')
            df.to_excel(temp_path, index=False)
            
            # 如果临时文件保存成功，替换原文件
            if temp_path.exists():
                if output_path.exists():
                    output_path.unlink()
                temp_path.rename(output_path)
                return True
                
        except Exception as e:
            print(f"保存尝试 {attempt + 1} 失败: {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(2)
    return False

def validate_data_before_save(data):
    """保存前验证数据"""
    if not data or len(data) == 0:
        print("警告：没有数据需要保存")
        return False
    return True

if __name__ == "__main__":
    # 设置API密钥
    API_KEY = "sk-cwfevlobjjjhwdphodmqwtyzrtsfpwcwfujwbmezqmydanex"  # 请替换为您的实际API密钥
    
    # 设置监控目录
    MONITOR_DIR = "data/messages"  # 请确保此目录存在
    
    # 创建分析器实例
    analyzer = HistoricalMessageAnalyzer(API_KEY)
    
    # 开始监控
    print("启动消息监控系统...")
    analyzer.start_monitoring(MONITOR_DIR)