import json
import logging
import aiohttp
import asyncio
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List
import os

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.apidance.pro/sapi/UserTweets"
SEARCH_API_URL = "https://api.apidance.pro/sapi/Search"
API_KEY = "2e78k9hg7j2me2g1vhky7a5bh5r0r1"

async def get_user_tweets(user_id: str, cursor: str = "") -> Dict:
    """
    获取指定用户的推文列表
    
    参数:
        user_id: Twitter用户ID
        cursor: 分页游标（可选）
    
    返回:
        包含推文列表和下一页游标的字典
    """
    try:
        headers = {
            "apikey": API_KEY,
            "Content-Type": "application/json"
        }
        
        params = {
            "user_id": user_id
        }
        
        if cursor:
            params["cursor"] = cursor
        
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                API_URL,
                params=params,
                headers=headers,
                timeout=timeout
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"API返回数据: {data}")
                    
                    # 处理返回的数据
                    tweets = []
                    next_cursor = ""
                    
                    if isinstance(data, dict):
                        # 提取推文列表
                        if 'tweets' in data:
                            tweet_list = data['tweets']
                        elif 'data' in data and 'tweets' in data['data']:
                            tweet_list = data['data']['tweets']
                        else:
                            tweet_list = []
                            
                        # 提取下一页游标
                        if 'next_cursor' in data:
                            next_cursor = data['next_cursor']
                        elif 'data' in data and 'next_cursor' in data['data']:
                            next_cursor = data['data']['next_cursor']
                            
                        # 格式化推文数据
                        for tweet in tweet_list:
                            tweets.append({
                                'text': tweet.get('text', ''),
                                'created_at': tweet.get('created_at', ''),
                                'id': tweet.get('tweet_id', tweet.get('id', '')),
                                'retweet_count': tweet.get('retweet_count', 0),
                                'like_count': tweet.get('like_count', 0),
                                'reply_count': tweet.get('reply_count', 0)
                            })
                    
                    return {
                        'tweets': tweets,
                        'next_cursor': next_cursor
                    }
                    
                elif response.status == 429:  # Rate limit exceeded
                    error_text = await response.text()
                    logger.warning(f"遇到速率限制: {error_text}")
                    return {'tweets': [], 'next_cursor': ''}
                else:
                    error_text = await response.text()
                    logger.error(f"API请求失败: {response.status} - {error_text}")
                    return {'tweets': [], 'next_cursor': ''}
                    
    except Exception as e:
        logger.error(f"获取用户推文时出错: {str(e)}")
        return {'tweets': [], 'next_cursor': ''}

async def get_user_id_by_username(username: str) -> str:
    """
    通过用户名搜索获取用户ID
    
    参数:
        username: Twitter用户名（不包含@符号）
    
    返回:
        用户ID或空字符串（如果未找到）
    """
    try:
        headers = {
            "apikey": API_KEY,
            "Content-Type": "application/json"
        }
        
        params = {
            "q": f"from:{username}",
            "cursor": "",
            "sort_by": "Latest"
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                SEARCH_API_URL,
                params=params,
                headers=headers,
                timeout=timeout
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict):
                        tweets = data.get('tweets', []) or data.get('data', {}).get('tweets', [])
                        if tweets and len(tweets) > 0:
                            return tweets[0].get('user_id', '')
                    
                    logger.warning(f"未找到用户 {username} 的ID")
                    return ''
                else:
                    error_text = await response.text()
                    logger.error(f"搜索用户ID失败: {response.status} - {error_text}")
                    return ''
                    
    except Exception as e:
        logger.error(f"获取用户ID时出错: {str(e)}")
        return ''

async def get_all_user_tweets(user_id: str) -> List[Dict]:
    """
    获取用户的所有历史推文
    
    参数:
        user_id: Twitter用户ID
    
    返回:
        包含所有推文的列表
    """
    all_tweets = []
    next_cursor = ""
    retry_count = 0
    max_retries = 3
    consecutive_empty_count = 0
    max_consecutive_empty = 3
    
    while True:
        try:
            result = await get_user_tweets(user_id, next_cursor)
            tweets = result['tweets']
            next_cursor = result['next_cursor']
            
            if tweets:
                all_tweets.extend(tweets)
                logger.info(f"已获取 {len(all_tweets)} 条推文")
                consecutive_empty_count = 0  # 重置连续空结果计数
            else:
                consecutive_empty_count += 1
                logger.warning(f"当前页面没有推文，连续空结果次数: {consecutive_empty_count}")
            
            # 如果连续多次获取到空结果，可能是API限制或错误
            if consecutive_empty_count >= max_consecutive_empty:
                if retry_count < max_retries:
                    retry_count += 1
                    logger.warning(f"连续获取到空结果，第 {retry_count} 次重试...")
                    await asyncio.sleep(5)  # 增加等待时间
                    consecutive_empty_count = 0
                    continue
                else:
                    logger.error("达到最大重试次数，停止获取")
                    break
            
            if not next_cursor:
                # 如果没有下一页游标，等待一段时间后再次尝试
                if retry_count < max_retries:
                    retry_count += 1
                    logger.warning(f"没有下一页游标，第 {retry_count} 次重试...")
                    await asyncio.sleep(5)
                    continue
                else:
                    logger.info("没有更多推文可获取")
                    break
            
            # 添加延时以避免触发API限制
            await asyncio.sleep(2)  # 增加延时到2秒
            
        except Exception as e:
            logger.error(f"获取推文时出错: {str(e)}")
            if retry_count < max_retries:
                retry_count += 1
                logger.warning(f"发生错误，第 {retry_count} 次重试...")
                await asyncio.sleep(5)
                continue
            else:
                logger.error("达到最大重试次数，停止获取")
                break
    
    logger.info(f"总共获取到 {len(all_tweets)} 条推文")
    return all_tweets

def export_to_excel(tweets: List[Dict], username: str):
    """
    将推文数据导出到Excel文件
    
    参数:
        tweets: 推文列表
        username: Twitter用户名
    """
    if not tweets:
        logger.warning("没有推文数据可导出")
        return
        
    # 创建DataFrame
    df = pd.DataFrame(tweets)
    
    # 生成文件名（包含时间戳）
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"tweets_{username}_{timestamp}.xlsx"
    
    # 获取桌面路径
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    filepath = os.path.join(desktop, filename)
    
    # 导出到Excel
    df.to_excel(filepath, index=False, engine='openpyxl')
    logger.info(f"数据已导出到文件: {filepath}")

async def main():
    # 测试获取用户ID和推文
    username = "Zixi41620514"  # 示例用户名
    logger.info("开始测试API...")
    logger.info(f"测试用户名: {username}")
    
    try:
        # 先获取用户ID
        user_id = await get_user_id_by_username(username)
        if not user_id:
            logger.error(f"未能找到用户 {username} 的ID")
            return
            
        logger.info(f"找到用户ID: {user_id}")
        
        # 获取所有历史推文
        logger.info("开始获取所有历史推文...")
        all_tweets = await get_all_user_tweets(user_id)
        
        if all_tweets:
            logger.info(f"成功获取 {len(all_tweets)} 条推文")
            # 导出到Excel
            export_to_excel(all_tweets, username)
        else:
            logger.error("获取失败或未找到推文")
            
    except Exception as e:
        logger.error(f"测试时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 