import pandas as pd
import requests
import json
from typing import List, Dict
import time
from requests.exceptions import RequestException
import random
import os
import datetime

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
        model: str = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B",
        max_tokens: int = 512,
        temperature: float = 0.7,
        stream: bool = False,
        max_retries: int = 8,  # 最大重试次数
        initial_delay: float = 1.5  # 初始延迟时间（秒）
    ) -> Dict:
        url = f"{self.base_url}/chat/completions"
        
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": stream
        }

        attempt = 0
        while True:
            try:
                # 增加超时时间到30秒
                response = requests.post(
                    url, 
                    headers=self.headers, 
                    json=payload, 
                    timeout=30,  # 修改这里
                    verify=True  # 确保SSL验证
                )
                
                # 如果请求成功
                if response.status_code == 200:
                    return response.json()
                
                # 如果是API限制相关的错误（429或其他可重试的状态码）
                if response.status_code in [429, 500, 502, 503, 504]:
                    attempt += 1
                    if attempt > max_retries:
                        print(f"达到最大重试次数 {max_retries}，最后一次错误: {response.text}")
                        return None
                    
                    # 计算延迟时间（指数退避 + 随机抖动）
                    delay = initial_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    print(f"请求被限制，等待 {delay:.2f} 秒后进行第 {attempt} 次重试...")
                    time.sleep(delay)
                    continue
                
                # 其他错误
                response.raise_for_status()
                
            except RequestException as e:
                attempt += 1
                if attempt > max_retries:
                    print(f"达到最大重试次数 {max_retries}，最后一次错误: {str(e)}")
                    return None
                
                delay = initial_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                print(f"请求出错: {str(e)}，等待 {delay:.2f} 秒后进行第 {attempt} 次重试...")
                time.sleep(delay)
                continue

    def process_post(self, post_text):
        """
        处理帖子内容，提取KOL信息和推文内容
        """
        try:
            text = post_text.strip('{"text":"').strip('"}')
            
            if ' 发布新推文' not in text:
                print(f"内容格式不正确，缺少'发布新推文'标记: {text}")
                return None, None, None, None
                
            # 初始化分组类型
            group_type = None
            
            # 处理新格式：移除 "[KOL] " 前缀并标记分组
            if text.startswith('[KOL] '):
                text = text[6:]  # 移除前6个字符 "[KOL] "
                group_type = 'kol'
                
            kol_parts = text.split(' 发布新推文')
            if len(kol_parts) < 2:
                print(f"无法提取KOL名称: {text}")
                return None, None, None, None
            kol_name = kol_parts[0]
            
            remaining_text = kol_parts[1]
            remaining_text = remaining_text.replace('\\n', '\n')
            
            content_parts = remaining_text.strip().split('\n')
            
            link = None
            tweet_parts = []
            for part in content_parts:
                if part.strip().startswith('https://x.com/'):
                    link = part.strip()
                else:
                    tweet_parts.append(part.strip())
            
            tweet = ' '.join(tweet_parts).strip()
            
            return kol_name, tweet, link, group_type
        except Exception as e:
            print(f"Error processing post: {e}")
            print(f"问题内容: {post_text}")
            return None, None, None, None

    def analyze_tweet(self, tweet: str) -> tuple:
        while True:  # 无限重试，直到成功
            try:
                prompt = """
                你是一个加密货币舆情分析专家，请按以下规则分析这条推文：

                1. 核心摘要：一句话用中文提炼核心内容，删除表情符号/@提及/话题标签，
                2. 资产：将代币符号（如$BTC）、关键数据（如价格/市值）和核心事件提炼出来
                3. 主分类：[项目推广/投资/经验总结/情绪表达/其他] 必须单选
                4. 情绪标签：[积极/消极/讽刺/中性/狂热] 可多选
                5. 特征标签：[Meme文化/监管动态/链上分析/交易策略/项目推广/市场预测/风险警示/行业叙事/技术解读/社区生态/资产配置/行情波动] 最多选3个

                请按以下格式返回结果：
                
                摘要：[核心内容]
                资产：[如涉及具体资产，填写代币符号或合约地址]
                分类：[单选主分类]
                情绪：[多选情绪，用逗号分隔]
                标签：[最多3个特征标签，用逗号分隔]
                

                推文内容：
                {tweet}
                """
                
                messages = [
                    {
                        "role": "user",
                        "content": prompt.format(tweet=tweet)
                    }
                ]
                
                response = self.chat_completion(messages)
                
                if response and 'choices' in response and len(response['choices']) > 0:
                    analysis = response['choices'][0]['message']['content']
                    
                    summary = ""
                    category = ""
                    sentiment = ""
                    tags = ""
                    asset = ""
                    
                    lines = analysis.strip().split('\n')
                    for line in lines:
                        line = line.strip()
                        if line.startswith('摘要：'):
                            summary = line.replace('摘要：', '').strip()
                        elif line.startswith('分类：'):
                            category = line.replace('分类：', '').strip()
                        elif line.startswith('情绪：'):
                            sentiment = line.replace('情绪：', '').strip()
                        elif line.startswith('标签：'):
                            tags = line.replace('标签：', '').strip()
                        elif line.startswith('资产：'):
                            asset = line.replace('资产：', '').strip()
                    
                    print(f"解析结果:")
                    print(f"摘要: {summary}")
                    print(f"分类: {category}")
                    print(f"情绪: {sentiment}")
                    print(f"标签: {tags}")
                    print(f"资产: {asset}")
                    
                    return summary, category, asset, sentiment, tags
                
                print("API 返回结果无效，准备重试...")
                time.sleep(3)
                
            except Exception as e:
                print(f"处理出错: {str(e)}，准备重试...")
                time.sleep(3)
                continue

def process_post(post_text):
    try:
        text = post_text.strip('{"text":"').strip('"}')
        
        if ' 发布新推文' not in text:
            print(f"内容格式不正确，缺少'发布新推文'标记: {text}")
            return None, None, None, None
            
        # 初始化分组类型
        group_type = None
        
        # 处理新格式：移除 "[KOL] " 前缀并标记分组
        if text.startswith('[KOL] '):
            text = text[6:]  # 移除前6个字符 "[KOL] "
            group_type = 'kol'
            
        kol_parts = text.split(' 发布新推文')
        if len(kol_parts) < 2:
            print(f"无法提取KOL名称: {text}")
            return None, None, None, None
        kol_name = kol_parts[0]
        
        remaining_text = kol_parts[1]
        remaining_text = remaining_text.replace('\\n', '\n')
        
        content_parts = remaining_text.strip().split('\n')
        
        link = None
        tweet_parts = []
        for part in content_parts:
            if part.strip().startswith('https://x.com/'):
                link = part.strip()
            else:
                tweet_parts.append(part.strip())
        
        tweet = ' '.join(tweet_parts).strip()
        
        return kol_name, tweet, link, group_type
    except Exception as e:
        print(f"Error processing post: {e}")
        print(f"问题内容: {post_text}")
        return None, None, None, None

# 主程序
def main():
    # 初始化 API 客户端
    api_key = "sk-uttscpjxlrjgcivqkfungdyorfxpjcigiaksyjijodjdbcat"
    client = SiliconFlowClient(api_key)

    # 添加命令行参数或配置选项
    use_timestamp = False  # 设置为 False 则更新原文件，True 则生成带时间戳的新文件
    
    input_file = "chat_history.xlsx"
    if use_timestamp:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/analysis_results/all_analysis_results_{timestamp}.xlsx"
    else:
        output_file = input_file
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：找不到输入文件 {input_file}")
        print("请确保 chat_history.xlsx 文件在当前目录下")
        return

    df = pd.read_excel(input_file, engine='openpyxl')

    # 更新时间过滤
    start_time = pd.Timestamp('2025-02-27 23:01:26')
    df['create_time'] = pd.to_datetime(df['create_time'])
    df = df[df['create_time'] >= start_time]
    
    total_rows = len(df)
    print(f"共找到 {total_rows} 条需要处理的数据")

    # 改进错误处理逻辑
    processed_data = []
    processed_count = 0  # 添加处理计数器
    try:
        if os.path.exists(output_file):
            try:
                processed_df = pd.read_excel(output_file, engine='openpyxl')
                processed_data = processed_df.to_dict('records')
                processed_count = len(processed_data)  # 记录已处理的数据数量
                print(f"成功读取已有的处理数据，共 {processed_count} 条记录")
            except Exception as e:
                print(f"读取已有数据文件失败: {str(e)}")
                print("将创建新的输出文件")
                try:
                    os.remove(output_file)
                except:
                    pass
    except Exception as e:
        print(f"检查输出文件时出错: {str(e)}")

    REQUEST_INTERVAL = 3

    for index, row in df.iterrows():
        # 更新进度显示，考虑已处理的数据数量
        current_progress = processed_count + index + 1
        print(f"处理第 {current_progress}/{total_rows} 条数据")
        try:
            kol_name, tweet, link, group_type = client.process_post(row['content'])
            if kol_name and tweet:
                if index > 0:
                    time.sleep(REQUEST_INTERVAL)
                    
                summary, category, asset, sentiment, tags = client.analyze_tweet(tweet)
                new_data = {
                    'KOL': kol_name,
                    'Tweet': tweet,
                    'Link': link,
                    'Summary': summary,
                    'Category': category,
                    'Asset': asset,
                    'Sentiment': sentiment,
                    'Tags': tags,
                    'Create Time': row['create_time'],
                    'Group': group_type
                }
                processed_data.append(new_data)
                
                # 每条数据处理完就保存
                temp_df = pd.DataFrame(processed_data)
                temp_df.to_excel(
                    output_file, 
                    index=False,
                    engine='openpyxl'
                )
                print(f"已更新原始文件: {output_file}")
                
        except Exception as e:
            print(f"处理第{index + 1}条数据时出错: {str(e)}")

    print(f"数据处理完成，共处理 {len(processed_data)} 条数据")

if __name__ == "__main__":
    main()
