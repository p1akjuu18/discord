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

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test())