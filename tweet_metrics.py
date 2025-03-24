import requests
import logging
import sys
import pandas as pd
import re
import json
import os
from typing import Dict, Optional
from datetime import datetime
import time

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建results目录
RESULTS_DIR = "results"
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

class TweetMetrics:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apidance.pro"
        self.headers = {
            "apikey": "2e78k9hg7j2me2g1vhky7a5bh5r0r1"
        }
        self.metrics_cache = {}

    def extract_tweet_id(self, url: str) -> Optional[str]:
        """从推文URL中提取ID"""
        try:
            # 匹配status/后面的数字
            pattern = r'status/(\d+)'
            match = re.search(pattern, url)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None

    def get_tweet_metrics(self, tweet_url: str, time_point: str = None) -> Optional[Dict]:
        """
        获取推文的互动数据
        
        Args:
            tweet_url: 推文URL
            time_point: 时间点标识（'initial', '3min' 或 '10min'）
            
        Returns:
            包含互动数据的字典，如果失败则返回None
        """
        try:
            # 提取推文ID
            tweet_id = self.extract_tweet_id(tweet_url)
            if not tweet_id:
                logger.error(f"无法从URL提取推文ID: {tweet_url}")
                return None

            # 构建API请求URL
            url = f"{self.base_url}/sapi/TweetDetail"
            params = {
                "tweet_id": tweet_id,
                "cursor": None
            }
            
            logger.info(f"请求URL: {url}")
            logger.info(f"请求参数: {params}")
            
            # 发送请求
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                json_data = response.json()
                logger.info(f"API响应: {json_data}")
                if "tweets" in json_data and json_data["tweets"]:
                    tweet_data = json_data["tweets"][0]
                    metrics = {
                        "likes": tweet_data.get("favorite_count", 0),
                        "retweets": tweet_data.get("retweet_count", 0),
                        "replies": tweet_data.get("reply_count", 0),
                        "quotes": tweet_data.get("quote_count", 0)
                    }
                    logger.info(f"成功获取推文数据: {metrics}")

                    # 缓存数据
                    if time_point:
                        cache_key = f"{tweet_id}_{time_point}"
                        self.metrics_cache[cache_key] = metrics

                    return metrics
                else:
                    logger.error("API响应中没有tweets数据")
                    return None
            else:
                logger.error(f"API请求失败: {response.status_code} - {response.text}")
                return None
                    
        except Exception as e:
            logger.error(f"获取推文数据时发生错误: {str(e)}")
            logger.error("详细错误信息:", exc_info=True)
            return None

def process_excel_file():
    try:
        # 检查文件是否存在
        file_path = "processed_data0303.xlsx"
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return

        try:
            # 读取Excel文件
            df = pd.read_excel(file_path)
        except Exception as e:
            logger.error(f"读取Excel文件失败: {str(e)}")
            return
        
        # 创建results目录（如果不存在）
        if not os.path.exists(RESULTS_DIR):
            os.makedirs(RESULTS_DIR)
            logger.info(f"创建目录: {RESULTS_DIR}")
        
        # 打印列名
        logger.info("Excel文件的列名:")
        logger.info(df.columns.tolist())
        
        # 检查是否存在必要的列
        required_columns = ['Asset', 'Link']
        for col in required_columns:
            if col not in df.columns and col.lower() not in df.columns:
                logger.error(f"缺少必要的列: {col}")
                return
        
        # 获取实际的列名（考虑大小写）
        asset_col = 'Asset' if 'Asset' in df.columns else 'asset'
        link_col = 'Link' if 'Link' in df.columns else 'link'
        
        # 删除asset为空或"无"的行
        df = df[~df[asset_col].isin(['', '无'])]
        df = df.dropna(subset=[asset_col])
        
        # 初始化API客户端
        api_key = "2e78k9hg7j2me2g1vhky7a5bh5r0r1"
        tweet_metrics = TweetMetrics(api_key)
        
        # 为互动数据添加新列
        df['favorite_count'] = 0
        df['retweet_count'] = 0
        df['reply_count'] = 0
        df['quote_count'] = 0
        
        # 遍历处理每个推文链接
        retry_count = 0
        max_retries = 3
        
        for index, row in df.iterrows():
            if pd.notna(row[link_col]):
                logger.info(f"正在处理第 {index + 1} 行的推文...")
                
                # 添加重试逻辑
                while retry_count < max_retries:
                    metrics = tweet_metrics.get_tweet_metrics(row[link_col])
                    if metrics:
                        # 更新数据
                        df.at[index, 'favorite_count'] = metrics['likes']
                        df.at[index, 'retweet_count'] = metrics['retweets']
                        df.at[index, 'reply_count'] = metrics['replies']
                        df.at[index, 'quote_count'] = metrics['quotes']
                        
                        try:
                            # 每次成功获取数据后保存文件
                            latest_file = os.path.join(RESULTS_DIR, "twitter_metrics_latest.xlsx")
                            df.to_excel(latest_file, index=False)
                            logger.info(f"已保存最新数据到: {latest_file}")
                        except Exception as e:
                            logger.error(f"保存Excel文件失败: {str(e)}")
                        
                        retry_count = 0  # 重置重试计数
                        break
                    else:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.error(f"处理第 {index + 1} 行推文失败，已达到最大重试次数")
                            retry_count = 0  # 重置重试计数，继续处理下一条
                            break
                        logger.warning(f"重试第 {retry_count} 次...")
                        time.sleep(5)  # 重试前等待5秒
                
                # 每次请求后等待2秒
                time.sleep(2)
        
        # 程序完成时保存一个带时间戳的副本
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(RESULTS_DIR, f"twitter_metrics_{timestamp}.xlsx")
            df.to_excel(output_file, index=False)
            logger.info(f"所有数据处理完成，已保存到: {output_file}")
        except Exception as e:
            logger.error(f"保存最终Excel文件失败: {str(e)}")
        
    except Exception as e:
        logger.error(f"处理Excel文件时出错: {str(e)}")
        logger.error("错误详情:", exc_info=True)

def main():
    process_excel_file()

if __name__ == "__main__":
    main() 