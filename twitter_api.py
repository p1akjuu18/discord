import json
import logging
import aiohttp
import asyncio
import sys
from urllib.parse import quote
from typing import Dict, Optional, List

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.apidance.pro/sapi/Search"
API_TWEET_URL = "https://api.apidance.pro/sapi/TweetDetail"
API_KEY = "2e78k9hg7j2me2g1vhky7a5bh5r0r1"

async def search_tweets(query: str) -> List[Dict]:
    """
    搜索Twitter最近的推文
    返回格式化的推文列表
    """
    try:
        headers = {
            "apikey": API_KEY,
            "Content-Type": "application/json"
        }
        
        params = {
            "q": query,
            "cursor": "",
            "sort_by": "Latest"
        }
        
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
                    
                    # 转换为格式化的推文列表
                    tweets = []
                    if isinstance(data, dict):
                        # 检查是否有tweets字段
                        if 'tweets' in data:
                            tweet_list = data['tweets']
                        # 检查是否有data.tweets结构
                        elif 'data' in data and 'tweets' in data['data']:
                            tweet_list = data['data']['tweets']
                        else:
                            tweet_list = []
                            
                        for tweet in tweet_list:
                            tweets.append({
                                'text': tweet.get('text', ''),
                                'created_at': tweet.get('created_at', ''),
                                'id': tweet.get('tweet_id', tweet.get('id', ''))
                            })
                    return tweets
                elif response.status == 429:  # Rate limit exceeded
                    error_text = await response.text()
                    logger.warning(f"遇到速率限制: {error_text}")
                    return []
                else:
                    error_text = await response.text()
                    logger.error(f"API请求失败: {response.status} - {error_text}")
                    return []
                    
    except Exception as e:
        logger.error(f"搜索Twitter时出错: {str(e)}")
        return []

async def get_tweet_by_id(tweet_id: str) -> Optional[Dict]:
    """
    通过推文ID获取单条推文的详细信息
    
    Args:
        tweet_id: 推文ID
        
    Returns:
        包含推文信息的字典，如果出错则返回None
    """
    try:
        headers = {
            "apikey": API_KEY,
            "Content-Type": "application/json"
        }
        
        params = {
            "tweet_id": tweet_id
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                API_TWEET_URL,
                params=params,
                headers=headers,
                timeout=timeout
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"API返回数据: {data}")
                    
                    if isinstance(data, dict):
                        # 根据API返回的数据结构解析
                        if 'tweet' in data:
                            tweet_data = data['tweet']
                        elif 'data' in data and 'tweet' in data['data']:
                            tweet_data = data['data']['tweet']
                        else:
                            logger.error(f"无法解析API返回的推文数据: {data}")
                            return None
                            
                        return {
                            'text': tweet_data.get('text', ''),
                            'created_at': tweet_data.get('created_at', ''),
                            'id': tweet_data.get('tweet_id', tweet_data.get('id', '')),
                            'author': tweet_data.get('user', {}).get('username', ''),
                            'likes': tweet_data.get('favorite_count', 0),
                            'retweets': tweet_data.get('retweet_count', 0),
                            'media': tweet_data.get('media', [])
                        }
                    return None
                else:
                    error_text = await response.text()
                    logger.error(f"获取推文API请求失败: {response.status} - {error_text}")
                    return None
                    
    except Exception as e:
        logger.error(f"获取推文时出错: {str(e)}")
        return None

async def test():
    # 测试搜索功能
    query = "eth"
    logger.info("开始测试API...")
    logger.info(f"测试查询: {query}")
    
    try:
        result = await search_tweets(query)
        if result:
            logger.info(f"搜索结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            logger.info(f"找到 {len(result)} 条推文")
        else:
            logger.error("搜索失败或未找到推文")
    except Exception as e:
        logger.error(f"测试时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

    # 添加测试获取特定推文的代码
    tweet_id = "1902608673022595531"  # 从URL中提取的推文ID
    logger.info(f"测试获取推文ID: {tweet_id}")
    
    try:
        tweet = await get_tweet_by_id(tweet_id)
        if tweet:
            logger.info(f"推文内容: {json.dumps(tweet, indent=2, ensure_ascii=False)}")
        else:
            logger.error("获取推文失败")
    except Exception as e:
        logger.error(f"测试时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test())