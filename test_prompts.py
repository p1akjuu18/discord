import requests
import json
import time
import os
import pandas as pd
from typing import Dict, List
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import unquote, quote

class PromptTester:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        # 创建一个空的DataFrame来存储所有结果
        self.df = pd.DataFrame(columns=[
            '交易币种', '方向', '杠杆',
            '入场点位1', '入场点位2', '入场点位3',
            '止损点位1', '止损点位2', '止损点位3',
            '止盈点位1', '止盈点位2', '止盈点位3',
            '分析内容'
        ])

    def read_terminal_input(self) -> List[str]:
        """
        从终端读取输入的消息
        """
        try:
            print("\n请输入消息内容（输入'quit'或'q'结束输入）：")
            messages = []
            while True:
                message = input("> ")
                if message.lower() in ['quit', 'q']:
                    break
                if message.strip():  # 忽略空行
                    messages.append(message)
            
            print(f"\n共收到 {len(messages)} 条消息")
            return messages
        except Exception as e:
            print(f"读取输入错误: {e}")
            return None

    def analyze_messages(self, messages: List[str]):
        """
        逐条分析消息并保存为Excel
        """
        # 第一步：分析内容的prompt
        analysis_prompt = """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，如 BTC/USDT、ETH/USDT 等。

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
        json_prompt = '''
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
        
        for i, msg in enumerate(messages, 1):
            print(f"\n=== 分析第 {i} 条消息 ===")
            print(f"消息预览: {msg[:100]}...")
            
            # 第一步：获取初步分析结果
            first_response = self.test_completion(
                [{"role": "user", "content": analysis_prompt.format(content=msg)}],
                model="deepseek-ai/DeepSeek-V3",
                max_tokens=1024,
                temperature=0.7
            )
            
            if first_response and 'choices' in first_response:
                analysis_result = first_response['choices'][0]['message']['content']
                print("\n初步分析完成，正在整理为表格格式...")
                
                # 第二步：将分析结果转换为JSON格式
                second_response = self.test_completion(
                    [{"role": "user", "content": json_prompt.format(content=analysis_result)}],
                    model="deepseek-ai/DeepSeek-V3",
                    max_tokens=1024,
                    temperature=0.7
                )
                
                if second_response and 'choices' in second_response:
                    try:
                        # 解析JSON响应
                        content = second_response['choices'][0]['message']['content']
                        # 打印原始内容以便调试
                        print("\n收到的JSON内容:")
                        print(content)
                        print("\n尝试解析JSON...")
                        
                        # 尝试清理内容（移除可能的前后缀说明文字）
                        content = content.strip()
                        if content.find('[') >= 0 and content.rfind(']') >= 0:
                            content = content[content.find('['):content.rfind(']')+1]
                        
                        data = json.loads(content)
                        
                        # 如果返回的是单个币种的数据，转换为列表
                        if not isinstance(data, list):
                            data = [data]
                        
                        # 处理每个币种的数据
                        for coin_data in data:
                            row_data = {
                                '交易币种': coin_data.get('交易币种'),
                                '方向': coin_data.get('方向'),
                                '杠杆': coin_data.get('杠杆'),
                                '分析内容': coin_data.get('分析内容')
                            }
                            
                            # 处理多个点位
                            for j, price in enumerate(coin_data.get('入场点位', [])[:3], 1):
                                row_data[f'入场点位{j}'] = price
                            for j, price in enumerate(coin_data.get('止损点位', [])[:3], 1):
                                row_data[f'止损点位{j}'] = price
                            for j, price in enumerate(coin_data.get('止盈点位', [])[:3], 1):
                                row_data[f'止盈点位{j}'] = price
                            
                            # 添加到DataFrame
                            self.df = pd.concat([self.df, pd.DataFrame([row_data])], ignore_index=True)
                        
                        print("\n数据已添加到Excel表格")
                    except json.JSONDecodeError as e:
                        print(f"JSON解析错误: {e}")
                        print("错误的JSON内容:")
                        print(content)
                    except Exception as e:
                        print(f"处理数据时出错: {e}")
                else:
                    print("JSON格式转换失败")
            else:
                print(f"第 {i} 条消息分析失败")
            
            time.sleep(1)
        
        # 保存Excel文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = f"trading_analysis_{timestamp}.xlsx"
        self.df.to_excel(excel_file, index=False)
        print(f"\n分析结果已保存到: {excel_file}")
        
        # 打印DataFrame
        print("\n分析结果预览:")
        print(self.df)

    def test_completion(
        self,
        messages: List[Dict[str, str]],
        model: str = "deepseek-ai/DeepSeek-V3",
        max_tokens: int = 512,
        temperature: float = 0.7
    ) -> Dict:
        """
        发送测试请求到API
        """
        url = f"{self.base_url}/chat/completions"
        
        # 创建payload数据
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        try:
            print("\n发送API请求...")
            response = requests.post(url, headers=self.headers, json=payload)
            print(f"响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    print("\n成功获取响应!")
                    print(f"使用的tokens: {result.get('usage', {}).get('total_tokens', 'unknown')}")
                    print("\n响应内容:")
                    print(result['choices'][0]['message']['content'])
                return result
            else:
                print(f"请求失败: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            return None

    def run_prompt_test(
        self,
        prompt_template: str,
        test_cases: List[Dict[str, List[str]]],
        model: str = "deepseek-ai/DeepSeek-V3"
    ):
        """
        运行prompt测试
        """
        print(f"\n开始测试 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        print(f"使用模型: {model}")
        print("提示词模板:")
        print("-" * 30)
        print(prompt_template)
        print("-" * 30)

        for i, test_case in enumerate(test_cases, 1):
            print(f"\n测试用例 {i}:")
            print("输入推文:")
            for tweet in test_case['tweets']:
                print(f"- {tweet}")

            messages = [
                {
                    "role": "user",
                    "content": prompt_template.format(tweets="\n".join(test_case['tweets']))
                }
            ]

            response = self.test_completion(messages, model=model)
            
            if response and 'choices' in response and len(response['choices']) > 0:
                result = response['choices'][0]['message']['content']
                print("\n模型返回:")
                print(result)
            else:
                print("\n请求失败")

            print("-" * 50)
            time.sleep(1)  # 添加延迟

def main():
    # API配置
    api_key = "sk-bncjvceywtsagptfcmtxfugdidpqzzafeokhtlhvtohmyyfu"
    print("初始化 PromptTester...")
    tester = PromptTester(api_key)
    
    print("准备读取输入...")
    messages = tester.read_terminal_input()  # 更新这里的方法调用
    
    if messages:
        print(f"成功读取到 {len(messages)} 条消息")
        tester.analyze_messages(messages)
    else:
        print("未能成功读取消息")

if __name__ == "__main__":
    main() 