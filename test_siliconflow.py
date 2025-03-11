import requests
import json
import os
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import time
from feishu_deepseekapi import SiliconFlowClient
   
class SiliconFlowClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        model: str = "deepseek-ai/DeepSeek-V3",
        max_tokens: int = 512,
        temperature: float = 0.7,
        stream: bool = False
    ) -> Dict:
        """
        发送聊天完成请求到硅基流动API
        """
        url = f"{self.base_url}/chat/completions"
        
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": stream
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            return None

    def analyze_crypto_tweets(
        self,
        tweets: List[str],
        model: str = "deepseek-ai/DeepSeek-V3"
    ) -> Dict:
        """
        分析加密货币相关推文
        """
        prompt = """
        请分析以下关于某一代币的推文，并按照以下格式返回结果：
        
        叙事信息：[用一句话总结主要叙事]
        可持续性：[评估叙事可持续性]
        
        请忽略:
        - 纯价格预测
        - 无实质内容的喊单
        - 重复转发的同一条信息
        - 广告/抽奖/无关话题
        
        以下是需要分析的推文:
        {tweets}
        """
        
        messages = [
            {
                "role": "user",
                "content": prompt.format(tweets="\n".join(tweets))
            }
        ]
        
        return self.chat_completion(messages, model=model)

class APITester:
    def __init__(self, api_key="sk-otojtetvwuxonxxslyscpqksgeqemplmvzlndcpbnkdaexlu"):
        self.client = SiliconFlowClient(api_key)
        self.timeout = 30
        self.max_retries = 3
        self.retry_delay = 2

    def test_api_connection(self, content="测试消息"):
        """测试API连接"""
        print("开始测试API连接...")
        
        for attempt in range(self.max_retries):
            try:
                response = self.client.chat_completion(
                    messages=[
                        {"role": "user", "content": content}
                    ],
                    model="deepseek-ai/DeepSeek-V3",
                    max_tokens=1024,
                    temperature=0.7,
                    timeout=self.timeout
                )
                
                print("\n成功获得响应:")
                print(f"状态: 成功")
                print(f"响应内容: {response['choices'][0]['message']['content']}")
                return True
                
            except Exception as e:
                print(f"\n第 {attempt + 1} 次请求失败:")
                print(f"错误信息: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    print(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print("达到最大重试次数，测试失败")
                    return False

def main():
    # 直接设置API密钥
    api_key = "sk-otojtetvwuxonxxslyscpqksgeqemplmvzlndcpbnkdaexlu"
    client = SiliconFlowClient(api_key)

    # 读取Excel文件
    excel_path = os.path.join(os.path.expanduser("~"), "Desktop", "twitter_results.xlsx")
    df = pd.read_excel(excel_path)
    
    # 获取正确的列名
    keyword_column = '搜索关键词'  # 修改为实际的列名
    content_column = '推文内容'    # 这个列名是正确的
    
    # 创建结果列表
    analysis_results = []

    # 按关键词分组处理
    for keyword, group in df.groupby(keyword_column):
        # 获取该关键词的所有推文
        tweets = group[content_column].tolist()
        
        # 记录请求时间
        request_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n开始处理关键词: {keyword}")
        print(f"发现推文数量: {len(tweets)}")
        
        # 发送API请求
        response = client.analyze_crypto_tweets(tweets)
        
        if response and 'choices' in response and len(response['choices']) > 0:
            analysis = response['choices'][0]['message']['content']
            
            # 解析API返回的结果
            narrative = ""
            sustainability = ""
            
            # 分割返回的文本
            lines = analysis.strip().split('\n')
            for line in lines:
                if line.startswith('叙事信息：'):
                    narrative = line.replace('叙事信息：', '').strip()
                elif line.startswith('可持续性：'):
                    sustainability = line.replace('可持续性：', '').strip()
            
            # 保存结果（移除时间范围）
            analysis_results.append({
                '请求时间': request_time,
                '搜索关键词': keyword,
                '叙事信息': narrative,
                '可持续性评估': sustainability,
                '原始推文数量': len(tweets)
            })
            
            # 打印进度
            print(f"处理完成: {keyword}")
            print(f"叙事信息: {narrative}")
            print(f"可持续性: {sustainability}")
            print("-------------------")
        else:
            print(f"处理失败: {keyword}")
            print("API返回错误或无响应")
            print("-------------------")
        
        # 添加短暂延迟，避免请求过于频繁
        time.sleep(1)

    # 将结果保存为新的Excel文件
    results_df = pd.DataFrame(analysis_results)
    output_path = os.path.join(os.path.expanduser("~"), "Desktop", "crypto_analysis_results.xlsx")
    results_df.to_excel(output_path, index=False)
    print(f"\n分析结果已保存至: {output_path}")

    tester = APITester()
    
    # 测试简单消息
    print("=== 测试1: 发送简单消息 ===")
    tester.test_api_connection("你好，这是一条测试消息。")
    
    # 测试较长消息
    print("\n=== 测试2: 发送较长消息 ===")
    long_message = """
    BTC分析：
    目前比特币价格在72000附近，
    支撑位在70000，
    压力位在75000，
    建议在71000做多，
    止损69000，
    目标位77000。
    """
    tester.test_api_connection(long_message)

if __name__ == "__main__":
    main() 