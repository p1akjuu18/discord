from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import json
import time
import os
from datetime import datetime
import requests
from typing import List, Dict, Optional, Tuple
import pandas as pd

class SiliconFlowClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def chat_completion(self, messages, model="deepseek-ai/DeepSeek-V3", 
                       max_tokens=1024, temperature=0.7, timeout=30):
        """
        发送聊天请求到API
        """
        url = f"{self.base_url}/chat/completions"
        
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }

        try:
            response = requests.post(
                url, 
                headers=self.headers,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API请求失败: {str(e)}")

class MessageAnalyzer:
    def __init__(self, api_key="sk-otojtetvwuxonxxslyscpqksgeqemplmvzlndcpbnkdaexlu"):
        # 添加重试配置
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 30
        self.client = SiliconFlowClient(api_key)
        
        # 第一步：分析内容的prompt
        self.analysis_prompt = """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 可能会有多个入场点位
   - 记录所有提到的可能入场价格

5. **止损点位**：如果博主提到止损价格，请列出。
   - 记录所有提到的止损价位

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 记录所有提到的止盈价位
   - 通常会有点位1（求稳）和点位2，都需要列出

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。
   - 包含关键的支撑位和压力位
   - 包含趋势判断
   - 包含重要的技术指标分析

注意：
- 请确保所有价格都按要求进行补充
- 交易方向的判断必须严格按照上述规则执行

内容如下：
{content}
"""
        
        # 第二步：将分析结果转换为JSON的prompt
        self.json_prompt = '''
请将以下分析结果转换为标准JSON格式。严格按照以下要求：

1. 必须以[开头，以]结尾
2. 所有字符串必须使用双引号
3. 数字不需要引号
4. null值直接写null，不要加引号
5. 数组用[]表示
6. 确保返回的是可以被JSON解析的有效格式

示例格式：
[
    {{
        "交易币种": "BTC/USDT",
        "方向": "多单",
        "杠杆": 20,
        "入场点位": [91500, 92000, null],
        "止损点位": [90000, null, null],
        "止盈点位": [95000, 98000, null],
        "分析内容": "市场分析总结"
    }}
]

请将以下内容转换为上述JSON格式（注意：只返回JSON，不要包含其他说明文字）：

{content}
'''
        # 更新需要监控的频道列表
        self.channel_prompts = {
            "Drprofit": {"context_messages": 5},
            "Rose": {"context_messages": 5},
            "btc欧阳": {"context_messages": 5},
            "eliz": {"context_messages": 5},
            "shu": {"context_messages": 5},
            "tia-初塔": {"context_messages": 5},
            "trader": {"context_messages": 5},
            "traeep": {"context_messages": 5},
            "woods": {"context_messages": 5},
            "三木的交易日记": {"context_messages": 5},
            "三马合约": {"context_messages": 5},
            "三马现货": {"context_messages": 5},
            "交易员张张子": """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，一般会提到大饼=BTC=$btc，以太=ETH=$eth,SOL,BNB,DOGE。

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
""",
            "刘教练": {"context_messages": 5},
            "加密大漂亮": {"context_messages": 5},
            "大漂亮会员策略": {"context_messages": 5},
            "大镖客比特币行情": {"context_messages": 5},
            "打不死的交易员": {"context_messages": 5},
            "比特币军长": {"context_messages": 5},
            "皮皮虾": {"context_messages": 5},
            "舒琴分析": {"context_messages": 3},
            "舒琴实盘": {"context_messages": 5},
            "颜驰": {"context_messages": 5}
        }
        
        # 添加消息筛选规则
        self.default_filter = {
            "min_length": 10,
            "price_indicators": ['$', '美元', 'k', 'K', '千', '万'],
            "trading_keywords": ['多', '空', '做多', '做空', '买入', '卖出', '止损', '止盈'],
            "required_keywords": [],
            "excluded_keywords": []
        }

    def get_channel_name(self, file_path):
        """从文件路径中提取频道名称"""
        try:
            file_name = os.path.basename(file_path)
            parts = file_name.split('-')
            
            # 如果文件名包含多个部分，第一部分是ID，后面的都是频道名
            if len(parts) >= 2:
                # 去掉最后的.json后缀
                channel_name = '-'.join(parts[1:]).replace('.json', '')
                
                # 处理一些特殊情况
                if channel_name == 'shu-crypto':
                    return 'shu'
                # 可以添加其他特殊情况的处理
                
                return channel_name
                
            print(f"警告：无法从文件名 {file_name} 中提取频道名称")
            return None
            
        except Exception as e:
            print(f"解析文件名出错 {file_path}: {str(e)}")
            return None
    
    def get_message_history(self, data, num_messages):
        """获取最近的n条消息历史"""
        if 'messages' in data:
            messages = data['messages']
            return messages[-num_messages:] if len(messages) > num_messages else messages
        return []
    
    def analyze_content_with_retry(self, messages, prompt, max_retries=3):
        """添加重试机制的API调用"""
        for attempt in range(max_retries):
            try:
                response = self.client.chat_completion(
                    messages=messages,
                    model="deepseek-ai/DeepSeek-V3",
                    max_tokens=1024,
                    temperature=0.7,
                    timeout=self.timeout
                )
                return response
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    print(f"请求出错: {str(e)}，等待 {wait_time} 秒后进行第 {attempt + 1} 次重试...")
                    time.sleep(wait_time)
                else:
                    print(f"达到最大重试次数，请求失败: {str(e)}")
                    raise

    def should_analyze_message(self, content: str) -> bool:
        """判断消息是否需要分析"""
        if not content or len(content.strip()) < self.default_filter["min_length"]:
            return False
            
        # 检查是否包含价格相关信息
        has_price = any(indicator in content for indicator in self.default_filter["price_indicators"])
        
        # 检查是否包含交易相关词汇
        has_trading_terms = any(keyword in content.lower() for keyword in self.default_filter["trading_keywords"])
        
        return has_price or has_trading_terms

    def analyze_content(self, channel_name: str, messages: List[Dict]) -> Optional[str]:
        """分析消息内容"""
        try:
            # 获取最新的消息
            latest_message = messages[-1]
            content = latest_message.get('content', '')
            
            # 判断是否需要分析
            if not self.should_analyze_message(content):
                return None
                
            # 获取对应的提示词
            prompt = self.channel_prompts.get(channel_name, self.analysis_prompt)
            
            # 构建API请求
            messages = [{"role": "user", "content": prompt.format(content=content)}]
            
            # 调用API进行分析
            response = self.client.chat_completion(
                messages=messages,
                model="deepseek-ai/DeepSeek-V3",
                max_tokens=1024,
                temperature=0.7,
                timeout=self.timeout
            )
            
            if response and 'choices' in response:
                result = response['choices'][0]['message']['content']
                
                # 保存分析结果
                self.save_analysis_result(channel_name, {
                    'timestamp': latest_message.get('timestamp'),
                    'content': content,
                    'analysis': result
                })
                
                return result
                
        except Exception as e:
            print(f"分析内容时出错: {str(e)}")
            return None

    def save_analysis_result(self, channel_name: str, result: Dict):
        """保存分析结果"""
        try:
            # 使用绝对路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(current_dir, "analysis_results")
            
            # 创建输出目录
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{output_dir}/{channel_name}_{timestamp}.json"
            
            # 保存JSON结果
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
                
            # 更新Excel汇总文件
            excel_file = os.path.join(output_dir, f"{channel_name}_analysis.xlsx")
            df = pd.DataFrame([result])
            
            if os.path.exists(excel_file):
                existing_df = pd.read_excel(excel_file)
                df = pd.concat([existing_df, df], ignore_index=True)
                
            df.to_excel(excel_file, index=False)
            
            print(f"分析结果已保存到: {filename}")
            
        except Exception as e:
            print(f"保存分析结果失败: {str(e)}")

class MessageFileHandler(FileSystemEventHandler):
    def __init__(self, api_key):
        self.analyzer = MessageAnalyzer(api_key)
        self.processed_files = set()
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            self.process_file(event.src_path)
            
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            self.process_file(event.src_path)
    
    def process_file(self, file_path):
        try:
            # 确保文件写入完成
            time.sleep(1)
            
            # 检查文件是否已处理
            if file_path in self.processed_files:
                return
                
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            channel_name = self.analyzer.get_channel_name(file_path)
            if channel_name not in self.analyzer.channel_prompts:
                print(f"未配置的频道: {channel_name}")
                return
                
            config = self.analyzer.channel_prompts[channel_name]
            messages = self.analyzer.get_message_history(data, config['context_messages'])
            
            # 使用新的分析逻辑
            analysis = self.analyzer.analyze_content(channel_name, messages)
            if analysis:
                print(f"频道 {channel_name} 的分析已完成")
            
            # 标记文件为已处理
            self.processed_files.add(file_path)
            
        except json.JSONDecodeError:
            print(f"文件 {file_path} 解析失败")
        except Exception as e:
            print(f"处理文件 {file_path} 时发生错误: {str(e)}")
    
    def save_analysis(self, channel_name, analysis):
        """保存分析结果"""
        # 使用绝对路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(current_dir, "analysis_results")
        
        # 添加日志输出
        print(f"正在创建输出目录: {output_dir}")
        
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                print(f"成功创建输出目录: {output_dir}")
            except Exception as e:
                print(f"创建输出目录失败: {str(e)}")
                return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/{channel_name}_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(analysis)
            print(f"成功保存分析结果到: {filename}")
        except Exception as e:
            print(f"保存分析结果失败: {str(e)}")

def start_monitoring(path, api_key):
    event_handler = MessageFileHandler(api_key)
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

def test_analyzer():
    api_key = "sk-otojtetvwuxonxxslyscpqksgeqemplmvzlndcpbnkdaexlu"
    analyzer = MessageAnalyzer(api_key)
    
    # 更新数据文件夹路径
    data_dir = r"C:\Users\Admin\Desktop\discord-monitor-master031\data\messages"
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_results")
    
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 创建输出文件名（使用当前日期）
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(output_dir, f"trading_{current_date}.json")
    
    # 读取现有数据（如果文件存在）
    all_results = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            try:
                all_results = json.load(f)
            except json.JSONDecodeError:
                all_results = []
    
    # 获取所有json文件
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    for file_name in json_files[:1]:  # 先只处理第一个文件
        file_path = os.path.join(data_dir, file_name)
        channel_name = analyzer.get_channel_name(file_path)
        print(f"\n处理文件: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if 'messages' in data:
                messages = data['messages'][-5:]  # 只取最后5条消息
                
                for i, msg in enumerate(messages, 1):
                    print(f"\n处理第 {i} 条消息:")
                    print(f"原始内容: {msg['content']}")
                    
                    result = analyzer.analyze_content(channel_name, [msg])
                    if result:
                        # 解析原始JSON结果
                        analysis_data = json.loads(result)
                        
                        # 添加时间和博主信息
                        message_time = datetime.fromtimestamp(msg.get('timestamp', 0)).strftime("%Y-%m-%d %H:%M:%S")
                        enriched_data = {
                            "博主": channel_name,
                            "时间": message_time,
                            "分析": analysis_data
                        }
                        
                        # 添加到结果列表
                        all_results.append(enriched_data)
                        
                        # 保存所有结果到文件
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(all_results, f, ensure_ascii=False, indent=4)
                            
                        print(f"结果已追加到: {output_file}")
                    
        except Exception as e:
            print(f"处理文件时出错: {str(e)}")

if __name__ == "__main__":
    # 修改为正确的监控路径
    monitor_path = os.path.abspath(r"C:\Users\Admin\Desktop\discord-monitor-master031\data\messages")
    
    # 添加路径检查
    if not os.path.exists(monitor_path):
        print(f"错误：监控文件夹不存在: {monitor_path}")
        exit(1)
    
    print(f"开始监控文件夹: {monitor_path}")
    print(f"分析结果将保存在: {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'analysis_results')}")
    
    api_key = "sk-hlsotcqdjuyulkzsnkfzvjtgadaamrrupxskxjqjrhxdeftr"
    start_monitoring(monitor_path, api_key)

    test_analyzer()