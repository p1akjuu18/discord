import os
import json
import pandas as pd
import time
import uuid
from datetime import datetime
from pathlib import Path
import requests
from typing import Dict, List, Optional, Tuple
import re

class HistoricalDataProcessor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # 默认分析提示词
        self.default_prompt = """
你是一个专业的加密货币交易分析师。请仔细分析以下交易员给出的信息，给出其中的关键数据，提取并整理出以下信息：

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
        
        # 初始化空的channel_prompts字典
        self.channel_prompts = {}
        
        # 默认的消息筛选规则
        self.default_filter = {
            "min_length": 10,
            "price_indicators": ['$', '美元', 'k', 'K', '千', '万'],
            "trading_keywords": ['多', '空', '做多', '做空', '买入', '卖出', '止损', '止盈', 
                               'long', 'short', 'buy', 'sell', 'stop', 'target'],
            "required_keywords": [],
            "excluded_keywords": []
        }
        
        # 各频道的特定筛选规则
        self.channel_filters = {}
        
    def analyze_message(self, content: str, channel_name: str = None, retry_count: int = 3) -> Optional[Dict]:
        """分析单条消息"""
        # 获取对应频道的提示词，如果没有则使用默认提示词
        prompt = self.channel_prompts.get(channel_name, self.default_prompt)
        prompt = prompt.format(content=content)
        
        messages = [
            {"role": "system", "content": "你是一个专业的加密货币交易分析师。请仔细分析以下交易员给出的信息，给出其中的关键数据。"},
            {"role": "user", "content": prompt}
        ]
        
        for attempt in range(retry_count):
            try:
                # 根据尝试次数递增超时时间
                timeout = 30 + attempt * 10
                
                print(f"尝试分析 (第{attempt + 1}次), 超时设置: {timeout}秒")
                
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=self.headers,
                    json={
                        "model": "deepseek-ai/DeepSeek-V3",
                        "messages": messages,
                        "temperature": 0.7,
                        "max_tokens": 1024,
                        "top_p": 0.7,
                        "top_k": 50,
                        "frequency_penalty": 0.5,
                        "n": 1,
                        "stream": False,
                        "response_format": {
                            "type": "text"
                        }
                    },
                    timeout=timeout
                )
                response.raise_for_status()
                result = response.json()
                
                if 'choices' in result and len(result['choices']) > 0:
                    analysis = result['choices'][0]['message']['content']
                    return self._extract_trading_info(analysis)
                    
            except requests.exceptions.Timeout:
                wait_time = (2 ** attempt) * 3  # 指数退避策略
                print(f"请求超时 (第{attempt + 1}次尝试)。等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            except requests.exceptions.HTTPError as e:
                wait_time = (2 ** attempt) * 3
                print(f"HTTP错误 (第{attempt + 1}次尝试): {str(e)}。等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            except Exception as e:
                wait_time = (2 ** attempt) * 3
                print(f"第{attempt + 1}次尝试分析失败: {str(e)}。等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            
            if attempt == retry_count - 1:
                print("达到最大重试次数，分析失败")
                return None
        
        return None
    
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
    
    def _extract_trading_info(self, content: str) -> Dict:
        """从分析结果中提取交易信息，处理嵌套JSON问题"""
        # 初始化标准结构
        info = {
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
            "分析内容": ""
        }
        
        # 检查内容中是否包含JSON代码块
        json_data = None
        
        # 尝试提取JSON代码块
        if "```json" in content:
            try:
                json_str = content.split("```json")[1].split("```")[0].strip()
                json_data = json.loads(json_str)
            except:
                pass
        elif "```" in content:  # 尝试其他代码块格式
            try:
                match = re.search(r'```(.*?)```', content, re.DOTALL)
                if match:
                    json_str = match.group(1).strip()
                    json_data = json.loads(json_str)
            except:
                pass
        
        # 如果提取到了JSON数据，合并到结果中
        if json_data and isinstance(json_data, dict):
            # 合并JSON数据
            for key, value in json_data.items():
                if key in info:
                    info[key] = value
            
            # 如果JSON中也包含分析内容，使用它，否则使用原始内容
            if "分析内容" in json_data and json_data["分析内容"]:
                info["分析内容"] = json_data["分析内容"]
            else:
                # 移除掉代码块部分，保留其他文本作为分析内容
                clean_content = re.sub(r'```.*?```', '', content, flags=re.DOTALL).strip()
                if clean_content:
                    info["分析内容"] = clean_content
        else:
            # 如果没有提取到JSON，尝试从原始内容中提取基本信息
            info = self._extract_basic_trading_info(content)
        
        return info
    
    def _extract_basic_trading_info(self, content: str) -> Dict:
        """从分析结果中提取基本交易信息"""
        info = {
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
            "分析内容": content
        }
        
        # 简单关键词提取逻辑（可以根据需要扩展）
        if "多" in content and not "空" in content:
            info["方向"] = "多"
        elif "空" in content and not "多" in content:
            info["方向"] = "空单"
            
        # 尝试提取币种
        coin_patterns = [r'BTC', r'ETH', r'Eth', r'btc', r'eth']
        for pattern in coin_patterns:
            if re.search(pattern, content):
                info["交易币种"] = pattern.upper()
                break
        
        # 这里可以添加更多提取逻辑...
        
        return info
    
    def process_json_files(self, data_dir: str, output_dir: str):
        """处理所有JSON文件"""
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
        
        for i, file_path in enumerate(json_files, 1):
            print(f"\n处理文件 {i}/{total_files}: {file_path.name}")
            
            try:
                # 读取已存在的JSON结果文件，而不是处理原始数据
                channel_name = self._extract_channel_name(file_path.name)
                channel_results_file = os.path.join(output_dir, f"{channel_name}_results.json")
                
                if os.path.exists(channel_results_file):
                    with open(channel_results_file, 'r', encoding='utf-8') as f:
                        channel_results = json.load(f)
                        all_results.extend(channel_results)
                        print(f"已从{channel_results_file}加载{len(channel_results)}条结果")
                else:
                    print(f"警告：结果文件 {channel_results_file} 不存在")
                
                # 以下部分注释掉，不再处理原始数据
                """
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                channel_name = self._extract_channel_name(file_path.name)
                print(f"频道名称: {channel_name}")
                
                if isinstance(data, list):
                    messages = data
                elif isinstance(data, dict) and 'messages' in data:
                    messages = data['messages']
                else:
                    print(f"警告：文件 {file_path.name} 格式不正确")
                    continue
                
                print(f"发现 {len(messages)} 条消息")
                
                for j, msg in enumerate(messages, 1):
                    processed_messages += 1
                    content = msg.get('content', '')
                    
                    if not content:
                        skipped_messages += 1
                        continue
                    
                    print(f"\n处理消息 {j}/{len(messages)}")
                    result = self.analyze_message(content, channel_name)
                    
                    if result:
                        enriched_result = {
                            'channel': channel_name,
                            'timestamp': msg.get('timestamp'),
                            'message_id': msg.get('message_id'),
                            'author': msg.get('author'),
                            'author_id': msg.get('author_id'),
                            'original_content': content,
                            'attachments': msg.get('attachments', []),
                            'analysis': result
                        }
                        all_results.append(enriched_result)
                        
                        # 保存到频道特定的JSON文件
                        channel_file = os.path.join(output_dir, f"{channel_name}_results.json")
                        channel_results = []
                        
                        if os.path.exists(channel_file):
                            with open(channel_file, 'r', encoding='utf-8') as f:
                                channel_results = json.load(f)
                        
                        channel_results.append(enriched_result)
                        
                        with open(channel_file, 'w', encoding='utf-8') as f:
                            json.dump(channel_results, f, ensure_ascii=False, indent=2)
                """
                
                print(f"文件 {file_path.name} 处理完成")
                
            except Exception as e:
                print(f"处理文件时出错 {file_path}: {str(e)}")
        
        # 生成Excel报告
        if all_results:
            self._generate_excel_report(all_results, output_dir)
        
        print(f"\n处理完成:")
        print(f"处理了 {total_files} 个文件")
        print(f"加载了 {len(all_results)} 条记录")
    
    def _extract_channel_name(self, filename: str) -> str:
        """从文件名提取频道名称"""
        parts = filename.split('-')
        if len(parts) >= 2:
            return '-'.join(parts[1:]).replace('.json', '')
        return filename.replace('.json', '')
    
    def _generate_excel_report(self, results: List[Dict], output_dir: str):
        """生成Excel报告"""
        if not results:
            print("没有数据需要处理")
            return
        
        print(f"处理{len(results)}条分析记录...")
        
        # 转换为DataFrame
        df = pd.json_normalize(results, 
            sep='_',  # 使用下划线分隔嵌套字段
            meta=[
                'channel',
                'timestamp',
                'message_id',
                'author',
                'author_id',
                'original_content',  # 添加原始消息内容
                ['attachments'],
                ['analysis', '交易币种'],
                ['analysis', '方向'],
                ['analysis', '杠杆'],
                ['analysis', '入场点位1'],
                ['analysis', '入场点位2'],
                ['analysis', '入场点位3'],
                ['analysis', '止损点位1'],
                ['analysis', '止损点位2'],
                ['analysis', '止损点位3'],
                ['analysis', '止盈点位1'],
                ['analysis', '止盈点位2'],
                ['analysis', '止盈点位3'],
                ['analysis', '分析内容']
            ]
        )
        
        # 标准化数据
        if 'analysis_方向' in df.columns:
            df['analysis_方向'] = df['analysis_方向'].apply(self._standardize_direction)
        
        if 'analysis_交易币种' in df.columns:
            df['analysis_交易币种'] = df['analysis_交易币种'].apply(self._clean_currency)
        
        # 处理点位数据
        for col in df.columns:
            if any(keyword in col for keyword in ['入场点位', '止盈点位', '止损点位']):
                df[col] = df[col].apply(self._clean_position_value)
        
        # 处理列表类型
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"analysis_results_{timestamp}.xlsx")
        
        # 保存为Excel
        df.to_excel(output_path, index=False)
        print(f"\nExcel报告已生成：{output_path}")
    
    def _standardize_direction(self, value):
        """标准化交易方向"""
        if not value:
            return None
        value = str(value).lower()
        if value in ['多', 'long', '做多', '买入']:
            return '多'
        elif value in ['空', 'short', '做空', '卖出']:
            return '空'
        return value
    
    def _clean_currency(self, value):
        """清理币种名称"""
        if not value:
            return None
        value = str(value).upper()
        # 移除常见后缀
        value = value.replace('USDT', '').replace('USD', '').replace('USDC', '')
        return value.strip()
    
    def _clean_position_value(self, value):
        """清理点位数值"""
        if not value:
            return None
        try:
            # 移除货币符号和空格
            value = str(value).replace('$', '').replace(' ', '')
            return float(value)
        except:
            return None

def main():
    # 使用示例
    api_key = "sk-zacrufovtechzzjashtgqewnbclgmvdbxwegjoxpqvdlfbjb"  # 硅基流动API密钥
    processor = HistoricalDataProcessor(api_key)
    
    # 设置输入输出目录
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    data_dir = os.path.join(desktop_path, "1")  # 原始JSON文件目录
    output_dir = os.path.join(desktop_path, "1")  # 分析结果将保存在同一目录
    
    # 处理数据
    processor.process_json_files(data_dir, output_dir)

if __name__ == "__main__":
    main() 