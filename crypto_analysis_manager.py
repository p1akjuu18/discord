import requests
import json
import os
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import time

class CryptoAnalysisManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def chat_completion(self, messages: List[Dict[str, str]], model: str = "deepseek-ai/DeepSeek-V3") -> Dict:
        """基础API调用方法"""
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": 512,
            "temperature": 0.7,
            "stream": False
        }

        try:
            response = requests.post(f"{self.base_url}/chat/completions", 
                                   headers=self.headers, 
                                   json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            return None

    def analyze_narrative(self, tweets: List[str]) -> Dict:
        """分析叙事和可持续性"""
        prompt = """
        请分析以下关于某一代币的推文，并按照以下格式返回结果：
        
        叙事信息：[用一句话总结主要叙事]
        可持续性：[评估叙事可持续性]
        
        请忽略:
        - 纯价格预测
        - 无实质内容的喊单
        - 重复转发的同一条信息
        - 广告/抽奖/无关话题
        
        推文内容:
        {tweets}
        """
        return self._analyze(tweets, prompt)

    def analyze_risk(self, tweets: List[str]) -> Dict:
        """分析风险因素"""
        prompt = """
        请分析以下推文中的风险因素，并按照以下格式返回结果：
        
        主要风险：[一句话描述主要风险]
        风险等级：[高/中/低]
        建议对策：[具体建议]
        
        推文内容:
        {tweets}
        """
        return self._analyze(tweets, prompt)

    def analyze_sentiment(self, tweets: List[str]) -> Dict:
        """分析市场情绪"""
        prompt = """
        请分析以下推文的市场情绪，并按照以下格式返回结果：
        
        情绪指数：[1-10的数字，10最乐观]
        主导情绪：[恐慌/谨慎/中性/乐观/狂热]
        关注重点：[市场主要关注什么]
        
        推文内容:
        {tweets}
        """
        return self._analyze(tweets, prompt)

    def analyze_tech_development(self, tweets: List[str]) -> Dict:
        """分析技术发展"""
        prompt = """
        请分析以下推文中的技术发展信息，并按照以下格式返回结果：
        
        技术进展：[一句话总结主要技术进展]
        创新程度：[高/中/低]
        可信度：[高/中/低]
        
        推文内容:
        {tweets}
        """
        return self._analyze(tweets, prompt)

    def _analyze(self, tweets: List[str], prompt: str) -> Dict:
        """通用分析方法"""
        messages = [
            {
                "role": "user",
                "content": prompt.format(tweets="\n".join(tweets))
            }
        ]
        return self.chat_completion(messages)

def process_excel(input_file: str, analysis_type: str):
    """处理Excel文件并进行指定类型的分析"""
    api_key = "sk-ijfgzjxmxcbiqzznpusbwgitmwxvkiwyddabfxmapjontfbm"
    analyzer = CryptoAnalysisManager(api_key)
    
    # 读取Excel
    df = pd.read_excel(input_file)
    analysis_results = []
    
    # 选择分析方法
    analysis_methods = {
        'narrative': analyzer.analyze_narrative,
        'risk': analyzer.analyze_risk,
        'sentiment': analyzer.analyze_sentiment,
        'tech': analyzer.analyze_tech_development
    }
    
    analyze_method = analysis_methods.get(analysis_type)
    if not analyze_method:
        print(f"未知的分析类型: {analysis_type}")
        return
    
    # 按关键词分组处理
    for keyword, group in df.groupby('搜索关键词'):
        tweets = group['推文内容'].tolist()
        request_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n开始处理关键词: {keyword}")
        print(f"发现推文数量: {len(tweets)}")
        
        response = analyze_method(tweets)
        
        if response and 'choices' in response and len(response['choices']) > 0:
            result = response['choices'][0]['message']['content']
            
            # 保存结果
            analysis_results.append({
                '请求时间': request_time,
                '搜索关键词': keyword,
                '分析结果': result,
                '推文数量': len(tweets)
            })
            
            print(f"处理完成: {keyword}")
            print(f"分析结果: {result}")
            print("-------------------")
        
        time.sleep(1)

    # 保存结果
    results_df = pd.DataFrame(analysis_results)
    output_file = f"crypto_analysis_{analysis_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    output_path = os.path.join(os.path.expanduser("~"), "Desktop", output_file)
    results_df.to_excel(output_path, index=False)
    print(f"\n分析结果已保存至: {output_path}")

if __name__ == "__main__":
    # 使用示例
    input_file = os.path.join(os.path.expanduser("~"), "Desktop", "twitter_results.xlsx")
    
    # 可以选择不同的分析类型：'narrative', 'risk', 'sentiment', 'tech'
    process_excel(input_file, 'narrative') 