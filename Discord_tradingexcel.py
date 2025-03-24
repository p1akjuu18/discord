import os
import time
import json
import pandas as pd
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import re
from difflib import SequenceMatcher
import math  # 添加 math 模块
from datetime import datetime, timezone, timedelta

# 飞书API配置
APP_ID = "cli_a736cea2ff78100d"
APP_SECRET = "C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS"
APP_TOKEN = "DEgMbxV8da9qWxsIKFlcI5PNn7g"
TABLE_ID = "tbliP3nxiZCa4rAH"
EXCEL_FILE = 'data/analysis_results/all_analysis_results.xlsx'
PROCESSED_IDS_FILE = 'data/processed_message_ids.json'  # 用于存储已处理的消息ID

def load_processed_ids():
    """加载已处理的消息ID"""
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, 'r', encoding='utf-8') as f:
                ids = json.load(f)
                # 过滤掉无效ID
                valid_ids = [id for id in ids if id and id.lower() != 'nan']
                return set(valid_ids)
        return set()
    except Exception as e:
        print(f"加载已处理ID失败: {str(e)}")
        return set()

def save_processed_ids(processed_ids):
    """保存已处理的消息ID"""
    try:
        os.makedirs(os.path.dirname(PROCESSED_IDS_FILE), exist_ok=True)
        with open(PROCESSED_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(processed_ids), f, ensure_ascii=False)
    except Exception as e:
        print(f"保存已处理ID失败: {str(e)}")

class FeishuAPI:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"
        self.access_token = None
        self.token_expires = 0
        self.existing_messages = set()  # 用于存储已有的消息ID

    def get_access_token(self):
        """获取访问令牌"""
        if self.access_token and time.time() < self.token_expires:
            return self.access_token

        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        response = requests.post(url, headers=headers, json=data)
        result = response.json()

        if result.get("code") == 0:
            self.access_token = result.get("tenant_access_token")
            self.token_expires = time.time() + result.get("expire") - 60  # 提前60秒更新
            return self.access_token
        else:
            raise Exception(f"获取访问令牌失败: {result}")

    def get_table_fields(self, app_token, table_id):
        """获取表格字段信息"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers)
            result = response.json()
            
            if result.get("code") == 0:
                fields = result.get("data", {}).get("items", [])
                print("\n表格字段信息:")
                for field in fields:
                    field_name = field.get("field_name", "")
                    field_type = field.get("type", "")
                    print(f"字段名: {field_name}, 类型: {field_type}")
                return fields
            else:
                raise Exception(f"获取字段信息失败: {result}")
                
        except Exception as e:
            raise Exception(f"获取字段信息失败: {str(e)}")

    def batch_create_records(self, app_token, table_id, records):
        """批量创建记录"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json"
        }
        
        # 首先获取表格实际字段
        print("\n获取飞书表格实际字段...")
        actual_fields = []
        try:
            fields_info = self.get_table_fields(app_token, table_id)
            actual_fields = [field.get("field_name") for field in fields_info]
            field_types = {field.get("field_name"): field.get("type") for field in fields_info}
            print(f"表格实际字段: {actual_fields}")
            print(f"字段类型: {field_types}")
        except Exception as e:
            print(f"获取表格字段失败: {str(e)}")
            print("将尝试使用默认字段映射")
        
        # 检查字段类型，查看哪些是日期时间类型
        date_fields = []
        if 'field_types' in locals() and field_types:
            date_fields = [field for field, type_id in field_types.items() if type_id == 5]  # 类型5是日期时间
            print(f"检测到日期时间字段: {date_fields}")
        else:
            # 默认假设"时间戳"和"日期时间"是日期时间字段
            date_fields = ["时间戳", "日期时间"]
        
        # 字段名映射：将Excel字段名映射到飞书表格字段名
        field_mapping = {
            # 基本字段
            'channel': '频道名称',
            'timestamp': '时间戳',
            'message_id': '消息ID',
            'author': '作者',
            'author_id': '作者ID',
            'attachments': '附件',
            'datetime': '日期时间',
            
            # 分析字段 - 移除'analysis.'前缀
            'analysis.交易币种': '币种',
            'analysis.方向': '交易方向',
            'analysis.杠杆': '杠杆',
            'analysis.入场点位1': '入场点位1',
            'analysis.入场点位2': '入场点位2',
            'analysis.入场点位3': '入场点位3',
            'analysis.止损点位1': '止损点位1',
            'analysis.止损点位2': '止损点位2',
            'analysis.止损点位3': '止损点位3',
            'analysis.止盈点位1': '止盈点位1',
            'analysis.止盈点位2': '止盈点位2',
            'analysis.止盈点位3': '止盈点位3',
            'analysis.分析内容': '分析内容',
            'analysis.原文': '消息内容',
            'analysis.翻译': '翻译内容'
        }
        
        # 如果获取到了实际字段，验证并调整映射
        if actual_fields:
            valid_mapping = {}
            for source, target in field_mapping.items():
                if target in actual_fields:
                    valid_mapping[source] = target
                else:
                    print(f"警告: 字段 '{target}' 在飞书表格中不存在")
            
            # 更新映射
            field_mapping = valid_mapping
            print(f"有效字段映射: {field_mapping}")
        
        # 应用字段映射
        mapped_records = []
        for record in records:
            mapped_record = {}
            for key, value in record.items():
                # 如果键在映射中，使用映射后的名称
                if key in field_mapping:
                    mapped_record[field_mapping[key]] = value
            mapped_records.append(mapped_record)
        
        # 使用映射后的记录
        records = mapped_records
        print(f"已完成字段映射处理")
        if records:
            print(f"映射后的记录示例: {records[0]}")
        
        # 清理和转换记录
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            for key, value in record.items():
                # 检查浮点数值是否有效
                if isinstance(value, float) and (pd.isna(value) or math.isinf(value) if hasattr(math, 'isinf') else False):
                    cleaned_record[key] = None  # 将NaN和inf替换为None
                # 处理日期时间字段
                elif key in date_fields and value is not None:
                    try:
                        # 添加类型调试信息
                        print(f"处理时间戳字段 '{key}', 值: {value}, 类型: {type(value)}")
                        
                        # 如果是 pandas Timestamp 对象直接处理
                        if str(type(value)).find('pandas') >= 0 and str(type(value)).find('Timestamp') >= 0:
                            print(f"检测到 pandas Timestamp 对象")
                            # 转换为 UTC 并获取毫秒时间戳
                            utc_dt = pd.to_datetime(value).tz_localize(None) - timedelta(hours=8)
                            cleaned_record[key] = int(utc_dt.timestamp() * 1000)
                            
                        # 如果是 datetime 对象直接处理
                        elif isinstance(value, datetime):
                            print(f"检测到 datetime 对象")
                            utc_dt = value - timedelta(hours=8)
                            cleaned_record[key] = int(utc_dt.timestamp() * 1000)
                            
                        # 如果已经是时间戳（整数或浮点数），直接使用
                        elif isinstance(value, (int, float)):
                            if pd.isna(value) or math.isinf(value):
                                cleaned_record[key] = None
                                print(f"检测到无效数值，设置为 None")
                            else:
                                # 确保时间戳是整数
                                ts = int(value)
                                # 将时间戳转换为UTC时间
                                local_dt = datetime.fromtimestamp(ts)
                                utc_dt = local_dt - timedelta(hours=8)  # 转换为UTC时间
                                cleaned_record[key] = int(utc_dt.timestamp() * 1000)
                                print(f"转换整数/浮点数时间戳成功: {cleaned_record[key]}")
                                
                        # 如果是字符串，尝试转换
                        elif isinstance(value, str):
                            if value.strip() == '':
                                cleaned_record[key] = None
                                print(f"检测到空字符串，设置为 None")
                            else:
                                print(f"正在转换字符串时间戳: {value}")
                                # 尝试直接解析时间戳字符串
                                try:
                                    ts = int(float(value))
                                    # 将时间戳转换为UTC时间
                                    local_dt = datetime.fromtimestamp(ts)
                                    utc_dt = local_dt - timedelta(hours=8)  # 转换为UTC时间
                                    cleaned_record[key] = int(utc_dt.timestamp() * 1000)
                                    print(f"解析字符串为时间戳成功: {cleaned_record[key]}")
                                except ValueError:
                                    # 如果不是时间戳字符串，尝试解析日期时间
                                    try:
                                        local_dt = pd.to_datetime(value)
                                        utc_dt = local_dt - timedelta(hours=8)  # 转换为UTC时间
                                        # 转换为毫秒级时间戳
                                        timestamp = int(utc_dt.timestamp() * 1000)
                                        print(f"解析字符串为日期时间成功: {timestamp}")
                                        cleaned_record[key] = timestamp
                                    except:
                                        # 如果所有转换都失败，使用当前时间
                                        print(f"无法解析字符串时间戳 '{value}'，使用当前时间")
                                        now = datetime.now() - timedelta(hours=8)  # 转换为UTC时间
                                        cleaned_record[key] = int(now.timestamp() * 1000)
                        else:
                            # 所有其他情况，使用当前时间
                            now = datetime.now() - timedelta(hours=8)  # 转换为UTC时间
                            cleaned_record[key] = int(now.timestamp() * 1000)
                            print(f"无法识别的时间戳类型 {type(value)}，使用当前时间")
                    except (ValueError, TypeError) as e:
                        print(f"日期时间转换错误 '{value}': {str(e)}")
                        # 出错时使用当前时间
                        now = datetime.now() - timedelta(hours=8)  # 转换为UTC时间
                        cleaned_record[key] = int(now.timestamp() * 1000)
                else:
                    cleaned_record[key] = value
            cleaned_records.append(cleaned_record)
        
        # 使用清理后的记录
        records = cleaned_records
        print(f"已清理无效浮点数和转换时间戳，处理后记录数: {len(records)}")
        
        try:
            # 每次最多创建500条记录
            batch_size = 500
            total_created = 0
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            print(f"\n总共有 {len(records)} 条记录需要创建，将分 {total_batches} 批处理")
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                data = {
                    "records": [{"fields": record} for record in batch]
                }
                
                current_batch = i//batch_size + 1
                print(f"\n处理第 {current_batch}/{total_batches} 批数据，本批数量: {len(batch)}")
                
                response = requests.post(url, headers=headers, json=data)
                result = response.json()
                
                if result.get("code") == 0:
                    created_count = len(result.get("data", {}).get("records", []))
                    total_created += created_count
                    print(f"第 {current_batch} 批成功创建 {created_count} 条记录")
                    print(f"当前总计已创建: {total_created} 条记录")
                else:
                    error_msg = f"第 {current_batch} 批创建失败，错误信息: {result}"
                    print(error_msg)
                    raise Exception(error_msg)
                
                # 添加短暂延迟，避免API限制
                if current_batch < total_batches:  # 最后一批不需要等待
                    time.sleep(1)
            
            print(f"\n全部处理完成！总共成功创建 {total_created} 条记录")
                
        except Exception as e:
            raise Exception(f"批量创建记录失败: {str(e)}")

    def get_existing_records(self, app_token, table_id):
        """获取已存在的记录"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json"
        }
        
        try:
            all_records = []
            page_token = None
            total_records = 0
            
            print("\n=== 开始获取现有记录 ===")
            
            while True:
                params = {"page_size": 100}
                if page_token:
                    params["page_token"] = page_token
                
                response = requests.get(url, headers=headers, params=params)
                result = response.json()
                
                if result.get("code") == 0:
                    records = result.get("data", {}).get("items", [])
                    all_records.extend(records)
                    total_records += len(records)
                    print(f"已获取 {total_records} 条记录...")
                    
                    # 获取下一页的token
                    page_token = result.get("data", {}).get("page_token")
                    if not page_token:
                        break
                else:
                    raise Exception(f"获取记录失败: {result}")
            
            # 清空现有消息ID集合
            self.existing_messages.clear()
            
            # 提取所有已存在的消息ID（使用正确的字段名）
            for record in all_records:
                message_id = record.get("fields", {}).get("消息ID")  # 使用飞书表格中的字段名
                if message_id:
                    self.existing_messages.add(str(message_id))  # 确保ID是字符串格式
            
            print(f"\n=== 现有记录统计 ===")
            print(f"总记录数: {len(all_records)}")
            print(f"有效消息ID数: {len(self.existing_messages)}")
            if self.existing_messages:
                print(f"消息ID示例: {list(self.existing_messages)[:3]}")
            
            return all_records
                
        except Exception as e:
            print(f"获取现有记录失败: {str(e)}")
            return []

def merge_similar_messages(df):
    """合并内容相似的消息"""
    print("\n=== 数据过滤详情 ===")
    print(f"初始数据行数: {len(df)}")
    
    # 按channel、币种和交易方向分组
    grouped = df.groupby(['channel', 'analysis.交易币种', 'analysis.方向'])
    
    # 统计分组情况
    print("\n分组统计:")
    for (channel, currency, direction), group in grouped:
        print(f"频道: {channel}, 币种: {currency}, 方向: {direction}, 记录数: {len(group)}")
    
    final_records = []  # 最终保留的记录
    merge_tracking = []  # 用于跟踪合并操作的记录
    
    for (channel, currency, direction), group in grouped:
        group = group.reset_index()
        print(f"\n处理组: 频道={channel}, 币种={currency}, 方向={direction}")
        
        # 如果组内只有一条记录，直接保存
        if len(group) == 1:
            final_records.append(group.iloc[0].to_dict())
            continue
        
        # 所有点位列
        point_columns = [
            'analysis.入场点位1', 'analysis.入场点位2', 'analysis.入场点位3',
            'analysis.止盈点位1', 'analysis.止盈点位2', 'analysis.止盈点位3',
            'analysis.止损点位1', 'analysis.止损点位2', 'analysis.止损点位3'
        ]
        
        # 将点位数据转换为数值型，无效数据设为None
        def convert_to_float(value):
            if pd.isna(value) or str(value).strip() in ['', '[]']:
                return None
            try:
                return float(str(value).strip('[]').strip())
            except (ValueError, TypeError):
                return None
        
        # 转换所有点位数据
        numeric_group = group.copy()
        for col in point_columns:
            numeric_group[col] = numeric_group[col].apply(convert_to_float)
        
        # 记录已处理的索引
        processed_indices = set()
        
        # 遍历每条记录，寻找可以合并的记录
        for i in range(len(numeric_group)):
            if i in processed_indices:
                continue
                
            current_record = numeric_group.iloc[i]
            similar_indices = []
            
            # 比较当前记录与其他记录
            for j in range(i + 1, len(numeric_group)):
                if j in processed_indices:
                    continue
                    
                compare_record = numeric_group.iloc[j]
                has_similar_points = False
                
                # 检查是否有相同的点位
                for col in point_columns:
                    val1 = current_record[col]
                    val2 = compare_record[col]
                    if val1 is not None and val2 is not None and abs(val1 - val2) < 0.0001:
                        has_similar_points = True
                        break
                
                if has_similar_points:
                    similar_indices.append(j)
            
            # 如果找到相似记录，进行合并
            if similar_indices:
                records_to_merge = [group.iloc[i]] + [group.iloc[j] for j in similar_indices]
                
                # 计算每条记录的有效点位数
                def count_valid_points(record):
                    return sum(1 for col in point_columns if not pd.isna(record[col]) and str(record[col]).strip() not in ['', '[]'])
                
                # 选择点位数据最多的记录作为基准
                base_record = max(records_to_merge, key=count_valid_points)
                merged_record = base_record.to_dict()
                
                # 收集所有有效点位
                entry_points = set()
                stop_points = set()
                profit_points = set()
                
                for record in records_to_merge:
                    # 收集入场点位
                    for k in range(1, 4):
                        value = record[f'analysis.入场点位{k}']
                        if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                            try:
                                entry_points.add(float(str(value).strip('[]').strip()))
                            except (ValueError, TypeError):
                                continue
                    
                    # 收集止损点位
                    for k in range(1, 4):
                        value = record[f'analysis.止损点位{k}']
                        if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                            try:
                                stop_points.add(float(str(value).strip('[]').strip()))
                            except (ValueError, TypeError):
                                continue
                    
                    # 收集止盈点位
                    for k in range(1, 4):
                        value = record[f'analysis.止盈点位{k}']
                        if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                            try:
                                profit_points.add(float(str(value).strip('[]').strip()))
                            except (ValueError, TypeError):
                                continue
                
                # 排序点位
                entry_points = sorted(entry_points)
                stop_points = sorted(stop_points)
                profit_points = sorted(profit_points)
                
                # 将点位分配到合并记录中
                for idx, value in enumerate(entry_points[:3], 1):
                    merged_record[f'analysis.入场点位{idx}'] = value
                for idx, value in enumerate(stop_points[:3], 1):
                    merged_record[f'analysis.止损点位{idx}'] = value
                for idx, value in enumerate(profit_points[:3], 1):
                    merged_record[f'analysis.止盈点位{idx}'] = value
                
                # 添加合并后的记录
                final_records.append(merged_record)
                
                # 记录合并操作详情
                merge_info = {
                    'channel': channel,
                    'currency': currency,
                    'direction': direction,
                    'merge_count': len(similar_indices) + 1,
                    'original_ids': [str(group.iloc[idx]['message_id']) for idx in [i] + similar_indices],
                    'merged_id': str(base_record['message_id']),
                    'merge_reason': "存在相同点位",
                    'original_records': [group.iloc[idx].to_dict() for idx in [i] + similar_indices],
                    'merged_record': merged_record
                }
                merge_tracking.append(merge_info)
                
                # 标记已处理的记录
                processed_indices.add(i)
                processed_indices.update(similar_indices)
                
                print(f"已合并 {len(similar_indices) + 1} 条记录")
            else:
                # 没有找到相似记录，直接保存
                final_records.append(group.iloc[i].to_dict())
                processed_indices.add(i)
    
    print(f"\n合并信息:")
    print(f"- 原始记录数: {len(df)}")
    print(f"- 最终记录数: {len(final_records)}")
    
    # 保存合并跟踪记录到Excel
    if merge_tracking:
        save_merge_tracking(merge_tracking)
    
    return pd.DataFrame(final_records)

def merge_message_records(messages):
    """合并多条相似消息的记录"""
    merged = {}
    # 使用第一条消息的基本信息
    base_message = messages[0]
    merged['channel'] = base_message['channel']
    
    # 确保时间戳正确处理
    # 1. 使用最新的时间戳
    valid_timestamps = [msg['timestamp'] for msg in messages if msg['timestamp'] is not None and not pd.isna(msg['timestamp'])]
    if valid_timestamps:
        merged['timestamp'] = max(valid_timestamps)
    else:
        # 如果没有有效时间戳，使用当前时间
        merged['timestamp'] = int(time.time())

    # 2. 处理日期时间字段
    valid_datetimes = []
    for msg in messages:
        if 'datetime' in msg and msg['datetime'] is not None and not pd.isna(msg['datetime']):
            try:
                dt = pd.to_datetime(msg['datetime'], format='mixed')
                valid_datetimes.append(dt)
            except:
                continue
    
    if valid_datetimes:
        merged['datetime'] = max(valid_datetimes)
    else:
        # 如果没有有效日期时间，使用当前时间
        merged['datetime'] = pd.to_datetime(datetime.now())
    
    # 其他字段保持不变
    merged['message_id'] = base_message['message_id']
    merged['analysis.交易币种'] = base_message['analysis.交易币种']
    merged['analysis.方向'] = base_message['analysis.方向']
    
    # 合并点位信息（去重）
    entry_points = set()
    stop_points = set()
    profit_points = set()
    
    for msg in messages:
        # 收集入场点位
        for i in range(1, 4):
            value = msg[f'analysis.入场点位{i}']
            if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                try:
                    entry_points.add(float(str(value).strip('[]').strip()))
                except (ValueError, TypeError):
                    continue
        
        # 收集止损点位
        for i in range(1, 4):
            value = msg[f'analysis.止损点位{i}']
            if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                try:
                    stop_points.add(float(str(value).strip('[]').strip()))
                except (ValueError, TypeError):
                    continue
        
        # 收集止盈点位
        for i in range(1, 4):
            value = msg[f'analysis.止盈点位{i}']
            if not pd.isna(value) and str(value).strip() not in ['', '[]']:
                try:
                    profit_points.add(float(str(value).strip('[]').strip()))
                except (ValueError, TypeError):
                    continue
    
    # 将合并后的点位分配到对应字段
    entry_points = sorted(entry_points)
    stop_points = sorted(stop_points)
    profit_points = sorted(profit_points)
    
    for i in range(3):
        merged[f'analysis.入场点位{i+1}'] = entry_points[i] if i < len(entry_points) else None
        merged[f'analysis.止损点位{i+1}'] = stop_points[i] if i < len(stop_points) else None
        merged[f'analysis.止盈点位{i+1}'] = profit_points[i] if i < len(profit_points) else None
    
    # 合并分析内容和原文
    analysis_contents = []
    original_contents = []
    for msg in messages:
        if not pd.isna(msg['analysis.分析内容']):
            analysis_contents.append(str(msg['analysis.分析内容']))
        if not pd.isna(msg['analysis.原文']):
            original_contents.append(str(msg['analysis.原文']))
    
    merged['analysis.分析内容'] = '\n---\n'.join(analysis_contents)
    merged['analysis.原文'] = '\n---\n'.join(original_contents)
    
    # 使用最高杠杆
    leverage = max((msg['analysis.杠杆'] for msg in messages if not pd.isna(msg['analysis.杠杆'])), default=None)
    merged['analysis.杠杆'] = leverage
    
    return merged

def save_merge_tracking(merge_tracking):
    """保存合并记录到Excel文件"""
    # 创建要保存的目录
    merge_dir = 'data/merge_records'
    try:
        os.makedirs(merge_dir, exist_ok=True)
        print(f"已创建或确认目录存在: {os.path.abspath(merge_dir)}")
    except Exception as e:
        print(f"创建目录失败: {str(e)}")
        # 尝试使用当前目录
        merge_dir = '.'
        print(f"将使用当前目录: {os.path.abspath(merge_dir)}")
    
    # 生成文件名，包含时间戳
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    file_path = f"{merge_dir}/merge_tracking_{timestamp}.xlsx"
    
    print(f"将保存合并记录到: {os.path.abspath(file_path)}")
    print(f"合并记录数量: {len(merge_tracking)}")
    
    # 准备数据框
    summary_data = []
    for i, record in enumerate(merge_tracking):
        summary_data.append({
            '合并组ID': i + 1,
            '频道': record['channel'],
            '币种': record['currency'],
            '合并消息数': record['merge_count'],
            '保留的消息ID': record['merged_id'],
            '合并的消息IDs': ', '.join(record['original_ids']),
            '合并时间': time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # 创建摘要表格
    summary_df = pd.DataFrame(summary_data)
    
    # 创建详细合并记录表格
    detail_data = []
    for i, record in enumerate(merge_tracking):
        # 为每个合并组添加原始记录
        for j, orig in enumerate(record['original_records']):
            row = {
                '合并组ID': i + 1,
                '记录类型': '原始记录' if j > 0 else '主记录(保留)',
                '消息ID': orig.get('message_id', ''),
                '频道': orig.get('channel', ''),
                '时间戳': orig.get('timestamp', ''),
                '币种': orig.get('analysis.交易币种', ''),
                '方向': orig.get('analysis.方向', ''),
                '入场点位1': orig.get('analysis.入场点位1', ''),
                '止损点位1': orig.get('analysis.止损点位1', ''),
                '止盈点位1': orig.get('analysis.止盈点位1', ''),
                '原始内容': str(orig.get('analysis.原文', ''))[:200] + '...' if len(str(orig.get('analysis.原文', ''))) > 200 else str(orig.get('analysis.原文', ''))
            }
            detail_data.append(row)
        
        # 添加合并后的记录
        merged = record['merged_record']
        row = {
            '合并组ID': i + 1,
            '记录类型': '合并结果',
            '消息ID': merged.get('message_id', ''),
            '频道': merged.get('channel', ''),
            '时间戳': merged.get('timestamp', ''),
            '币种': merged.get('analysis.交易币种', ''),
            '方向': merged.get('analysis.方向', ''),
            '入场点位1': merged.get('analysis.入场点位1', ''),
            '止损点位1': merged.get('analysis.止损点位1', ''),
            '止盈点位1': merged.get('analysis.止盈点位1', ''),
            '原始内容': str(merged.get('analysis.原文', ''))[:200] + '...' if len(str(merged.get('analysis.原文', ''))) > 200 else str(merged.get('analysis.原文', ''))
        }
        detail_data.append(row)
    
    detail_df = pd.DataFrame(detail_data)
    
    # 保存到Excel的不同工作表
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='合并摘要', index=False)
            detail_df.to_excel(writer, sheet_name='合并详情', index=False)
        
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"\n✅ 已成功保存合并记录到: {os.path.abspath(file_path)}")
            print(f"- 文件大小: {file_size/1024:.2f} KB")
            print(f"- 合并组数量: {len(summary_data)}")
            print(f"- 详细记录数量: {len(detail_data)}")
            return file_path
        else:
            print(f"\n❌ 文件似乎没有被保存: {file_path}")
            return None
    except Exception as e:
        print(f"\n❌ 保存文件时出错: {str(e)}")
        
        # 尝试使用不同的文件名和路径
        alternate_path = f"merge_tracking_{timestamp}.xlsx"
        print(f"尝试保存到替代位置: {os.path.abspath(alternate_path)}")
        try:
            with pd.ExcelWriter(alternate_path, engine='openpyxl') as writer:
                summary_df.to_excel(writer, sheet_name='合并摘要', index=False)
                detail_df.to_excel(writer, sheet_name='合并详情', index=False)
            
            if os.path.exists(alternate_path):
                print(f"✅ 成功保存到替代位置: {os.path.abspath(alternate_path)}")
                return alternate_path
            else:
                print("❌ 替代保存也失败了")
                return None
        except Exception as e2:
            print(f"❌ 替代保存也失败了: {str(e2)}")
            return None

def save_processing_step(df_kept, df_filtered, step_name):
    """
    保存数据处理步骤的结果
    df_kept: 保留的数据
    df_filtered: 被筛除的数据
    step_name: 处理步骤名称
    """
    # 创建保存目录
    save_dir = 'data/processing_steps'
    os.makedirs(save_dir, exist_ok=True)
    
    # 生成时间戳
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    
    # 保存保留的数据
    kept_file = f"{save_dir}/{timestamp}_{step_name}_kept.xlsx"
    df_kept.to_excel(kept_file, index=False)
    print(f"已保存保留的数据({len(df_kept)}行)到: {kept_file}")
    
    # 保存筛除的数据
    if df_filtered is not None and len(df_filtered) > 0:
        filtered_file = f"{save_dir}/{timestamp}_{step_name}_filtered.xlsx"
        df_filtered.to_excel(filtered_file, index=False)
        print(f"已保存筛除的数据({len(df_filtered)}行)到: {filtered_file}")

def merge_by_time_window(df, time_window_minutes=5):
    """基于时间窗口合并记录"""
    print("\n=== 基于时间窗口合并记录 ===")
    print(f"合并前记录数: {len(df)}")
    
    # 确保时间戳列是datetime类型
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 按channel、币种和交易方向分组
    grouped = df.groupby(['channel', 'analysis.交易币种', 'analysis.方向'])
    
    final_records = []
    merge_tracking = []
    
    for (channel, currency, direction), group in grouped:
        # 按时间排序
        group = group.sort_values('timestamp').reset_index(drop=True)
        
        # 初始化处理状态
        processed_indices = set()
        current_group = []
        
        for i in range(len(group)):
            if i in processed_indices:
                continue
            
            current_record = group.iloc[i]
            current_time = current_record['timestamp']
            current_group = [current_record]
            similar_indices = []
            
            # 寻找时间窗口内的记录
            for j in range(i + 1, len(group)):
                if j in processed_indices:
                    continue
                
                compare_record = group.iloc[j]
                compare_time = compare_record['timestamp']
                time_diff = (compare_time - current_time).total_seconds() / 60
                
                if time_diff <= time_window_minutes:
                    similar_indices.append(j)
                    current_group.append(compare_record)
                else:
                    # 由于记录是按时间排序的，如果超出时间窗口就可以停止检查
                    break
            
            # 如果找到需要合并的记录
            if similar_indices:
                # 合并记录
                merged_record = merge_message_records(current_group)
                final_records.append(merged_record)
                
                # 记录合并操作
                merge_info = {
                    'channel': channel,
                    'currency': currency,
                    'direction': direction,
                    'merge_count': len(current_group),
                    'original_ids': [str(record['message_id']) for record in current_group],
                    'merged_id': str(current_group[0]['message_id']),
                    'merge_reason': f"时间窗口({time_window_minutes}分钟)内的记录",
                    'original_records': [record.to_dict() for record in current_group],
                    'merged_record': merged_record
                }
                merge_tracking.append(merge_info)
                
                # 标记已处理的记录
                processed_indices.add(i)
                processed_indices.update(similar_indices)
                
                print(f"已合并 {len(current_group)} 条记录 (时间窗口: {time_window_minutes}分钟)")
            else:
                # 没有找到需要合并的记录，直接保存
                final_records.append(current_record.to_dict())
                processed_indices.add(i)
    
    # 转换回DataFrame
    result_df = pd.DataFrame(final_records)
    
    print(f"合并后记录数: {len(result_df)}")
    
    # 保存合并跟踪记录
    if merge_tracking:
        save_merge_tracking(merge_tracking)
    
    return result_df

def update_feishu_table(api, app_token, table_id):
    """更新飞书多维表格的主要流程"""
    try:
        print("\n=== 开始更新流程 ===")
        
        # 获取现有记录的消息ID
        print("\n获取已存在的记录...")
        api.get_existing_records(app_token, table_id)
        existing_ids = api.existing_messages
        print(f"已有记录数量: {len(existing_ids)}")
        
        # 读取已处理的消息ID
        processed_ids = load_processed_ids()
        print(f"本地已处理记录数量: {len(processed_ids)}")
        
        # 合并已存在的ID和已处理的ID
        all_processed_ids = existing_ids.union(processed_ids)
        print(f"所有已处理记录数量: {len(all_processed_ids)}")
        
        # 读取Excel文件
        print(f"\n1. 读取Excel文件: {EXCEL_FILE}")
        df = pd.read_excel(EXCEL_FILE, engine='openpyxl')
        initial_count = len(df)
        print(f"初始数据行数: {initial_count}")
        
        # 筛选出新记录
        df['message_id'] = df['message_id'].astype(str)
        new_df = df[~df['message_id'].isin(all_processed_ids)]
        print(f"新增记录数量: {len(new_df)}")
        
        if len(new_df) == 0:
            print("没有新的记录需要添加")
            return
            
        # 记录这批新记录的ID
        new_ids = set(new_df['message_id'].tolist())
        
        # 1. 删除代币种类为空的行
        print("\n=== 删除代币种类为空的行 ===")
        df_before = new_df.copy()
        new_df = new_df[new_df['analysis.交易币种'].notna() & (new_df['analysis.交易币种'].str.strip() != '')]
        filtered_currency = df_before[~df_before.index.isin(new_df.index)]
        
        if len(filtered_currency) > 0:
            print(f"删除了 {len(filtered_currency)} 行代币种类为空的数据")
            save_processing_step(new_df, filtered_currency, 'currency_empty_filtered')
        
        # 2. 删除所有点位均为空的行
        print("\n=== 删除所有点位均为空的行 ===")
        df_before = new_df.copy()
        
        # 所有点位列
        point_columns = [
            'analysis.入场点位1', 'analysis.入场点位2', 'analysis.入场点位3',
            'analysis.止盈点位1', 'analysis.止盈点位2', 'analysis.止盈点位3',
            'analysis.止损点位1', 'analysis.止损点位2', 'analysis.止损点位3'
        ]
        
        # 检查是否有任意点位不为空
        has_any_point = new_df[point_columns].notna().any(axis=1)
        
        # 保留至少有一个点位的行
        new_df = new_df[has_any_point]
        filtered_points = df_before[~df_before.index.isin(new_df.index)]
        
        if len(filtered_points) > 0:
            print(f"删除了 {len(filtered_points)} 行所有点位均为空的数据")
            save_processing_step(new_df, filtered_points, 'all_points_empty_filtered')
        
        # 3. 合并相似消息
        print("\n=== 合并相似消息 ===")
        df_before_merge = new_df.copy()
        new_df = merge_similar_messages(new_df)
        
        # 找出被合并的记录
        df_merged = df_before_merge[~df_before_merge['message_id'].isin(new_df['message_id'])]
        save_processing_step(new_df, df_merged, 'message_merging')
        
        # 4. 基于时间窗口合并记录
        print("\n=== 基于时间窗口合并记录 ===")
        df_before_time_merge = new_df.copy()
        new_df = merge_by_time_window(new_df, time_window_minutes=5)
        
        # 找出被时间窗口合并的记录
        df_time_merged = df_before_time_merge[~df_before_time_merge['message_id'].isin(new_df['message_id'])]
        save_processing_step(new_df, df_time_merged, 'time_window_merging')
        
        # 检查新增记录的时间戳字段
        print("\n检查新增记录的时间戳字段...")
        timestamp_missing = new_df['timestamp'].isna().sum()
        datetime_missing = new_df['datetime'].isna().sum()

        print(f"时间戳缺失数: {timestamp_missing}/{len(new_df)}")
        print(f"日期时间缺失数: {datetime_missing}/{len(new_df)}")

        # 对缺失时间戳的记录填充当前时间
        if timestamp_missing > 0:
            print("为缺失的时间戳字段填充当前时间...")
            current_timestamp = int(time.time())
            new_df.loc[new_df['timestamp'].isna(), 'timestamp'] = current_timestamp

        if datetime_missing > 0:
            print("为缺失的日期时间字段填充当前时间...")
            current_datetime = pd.to_datetime(datetime.now())
            new_df.loc[new_df['datetime'].isna(), 'datetime'] = current_datetime
        
        # 检查飞书表格字段定义
        print("\n检查飞书表格字段定义...")
        fields_info = api.get_table_fields(app_token, table_id)
        date_fields = [field.get("field_name") for field in fields_info if field.get("type") == 5]
        print(f"检测到的日期时间字段: {date_fields}")

        # 确认时间字段的实际映射
        actual_timestamp_field = None
        actual_datetime_field = None
        for field in fields_info:
            field_name = field.get("field_name", "")
            if "时间戳" in field_name:
                actual_timestamp_field = field_name
            if "日期时间" in field_name:
                actual_datetime_field = field_name

        print(f"实际时间戳字段名: {actual_timestamp_field}")
        print(f"实际日期时间字段名: {actual_datetime_field}")
        
        # 打印前三条记录的时间戳字段，了解数据状态
        print("\n检查记录时间戳字段（前3条）:")
        for i, row in new_df.head(3).iterrows():
            print(f"记录 {i}:")
            if 'timestamp' in new_df.columns:
                print(f"  - timestamp: {row['timestamp']} (类型: {type(row['timestamp'])})")
            if 'datetime' in new_df.columns:
                print(f"  - datetime: {row['datetime']} (类型: {type(row['datetime'])})")

        # 尝试转换pandas时间戳对象为Unix时间戳
        if 'datetime' in new_df.columns:
            print("将pandas时间戳对象转换为Unix时间戳...")
            for i, value in enumerate(new_df['datetime']):
                if str(type(value)).find('pandas') >= 0 and str(type(value)).find('Timestamp') >= 0:
                    try:
                        # 转换为Unix时间戳（秒）
                        unix_ts = int(value.timestamp())
                        new_df.at[i, 'datetime'] = unix_ts
                        print(f"转换pandas时间戳为Unix时间戳: {unix_ts}")
                    except:
                        print(f"转换失败: {value}, 类型: {type(value)}")
        
        # 检查记录中的时间戳字段
        if new_df.shape[0] > 0:
            sample = new_df.iloc[0].to_dict()
            print("\n记录字典中的时间字段:")
            for key, value in sample.items():
                if '时间' in key or 'time' in key.lower() or 'date' in key.lower():
                    print(f"- {key}: {value} (类型: {type(value)})")
            
            # 检查记录中是否存在缺失的时间戳
            missing_timestamp = 0
            for record in new_df.to_dict('records'):
                # 检查映射后的字段名
                time_fields = [k for k in record.keys() if '时间' in k or 'time' in k.lower() or 'date' in k.lower()]
                for field in time_fields:
                    if record[field] is None or (isinstance(record[field], float) and (pd.isna(record[field]) or math.isinf(record[field]))):
                        print(f"发现缺失时间戳字段 '{field}'，记录ID: {record.get('消息ID', 'unknown')}")
                        missing_timestamp += 1
                        # 填充当前时间
                        now = datetime.now() - timedelta(hours=8)  # 转换为UTC时间
                        record[field] = int(now.timestamp() * 1000)
                
            if missing_timestamp > 0:
                print(f"已修复 {missing_timestamp} 个缺失的时间戳字段")
        
        # 转换为记录格式并更新飞书表格
        records = new_df.to_dict('records')
        
        if records:
            # 创建记录
            api.batch_create_records(app_token, table_id, records)
            print(f"成功添加了 {len(records)} 条记录到飞书表格")
            
            # 更新已处理ID列表
            processed_ids.update(new_ids)
            save_processed_ids(processed_ids)
            print(f"已更新本地已处理ID记录，总数: {len(processed_ids)}")
        else:
            print("处理后没有需要添加的新记录")
            
    except Exception as e:
        print(f"更新失败: {str(e)}")
        import traceback
        traceback.print_exc()

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, api, app_token, table_id):
        self.api = api
        self.app_token = app_token
        self.table_id = table_id
        self.last_update = 0
        self.min_interval = 2  # 最小更新间隔（秒）
        self.last_content_hash = None  # 用于存储上次文件内容的哈希值
        
    def calculate_file_hash(self):
        """计算Excel文件内容的哈希值"""
        try:
            # 明确指定使用openpyxl引擎
            df = pd.read_excel(EXCEL_FILE, engine='openpyxl')
            return hash(str(df.to_dict()))
        except Exception as e:
            print(f"计算文件哈希值失败: {str(e)}")
            return None

    def check_for_updates(self):
        """检查Excel文件是否有实际内容更新"""
        current_time = time.time()
        if current_time - self.last_update < self.min_interval:
            return False
            
        current_hash = self.calculate_file_hash()
        if current_hash is None:
            return False
            
        if self.last_content_hash != current_hash:
            self.last_content_hash = current_hash
            self.last_update = current_time
            return True
            
        return False

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(EXCEL_FILE):
            if self.check_for_updates():
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 检测到文件内容变化，开始更新...")
                update_feishu_table(self.api, self.app_token, self.table_id)

def monitor_and_sync():
    """监控Excel文件变化并同步到飞书多维表格"""
    api = FeishuAPI(APP_ID, APP_SECRET)
    
    # 创建观察者和处理器
    event_handler = ExcelHandler(api, APP_TOKEN, TABLE_ID)
    observer = Observer()
    
    # 获取Excel文件所在的目录
    excel_dir = os.path.dirname(EXCEL_FILE)
    if not excel_dir:
        excel_dir = '.'
    
    # 开始监控
    observer.schedule(event_handler, excel_dir, recursive=False)
    observer.start()
    
    print(f"开始监控文件: {EXCEL_FILE}")
    print("程序正在运行中... 按Ctrl+C停止")
    
    try:
        # 首次运行立即更新一次
        update_feishu_table(api, APP_TOKEN, TABLE_ID)
        event_handler.last_content_hash = event_handler.calculate_file_hash()
        
        while True:
            # 每30秒主动检查一次文件内容
            if event_handler.check_for_updates():
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 定时检查发现文件内容变化，开始更新...")
                update_feishu_table(api, APP_TOKEN, TABLE_ID)
            time.sleep(30)
    except KeyboardInterrupt:
        observer.stop()
        print("\n停止监控")
    
    observer.join()

def get_field_info():
    """获取并显示表格字段信息"""
    api = FeishuAPI(APP_ID, APP_SECRET)
    try:
        fields = api.get_table_fields(APP_TOKEN, TABLE_ID)
        return fields
    except Exception as e:
        print(f"获取字段信息失败: {str(e)}")
        return None

if __name__ == '__main__':
    monitor_and_sync()