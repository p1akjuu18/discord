import json
import logging
import aiohttp
import asyncio
import sys
from urllib.parse import quote

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TwitterAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.apidance.pro"
        self.headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }

    async def search_tweets(self, query):
        try:
            params = {
                "q": query,
                "cursor": "",
                "sort_by": "Latest"
            }
            
            url = f"{self.base_url}/sapi/Search"
            logger.info(f"搜索关键词: {query}")
            
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    headers=self.headers,
                    timeout=timeout,
                    ssl=False
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 429:  # Rate limit exceeded
                        error_text = await response.text()
                        logger.warning(f"遇到速率限制: {error_text}")
                        return {
                            "errors": [
                                {
                                    "code": 429,
                                    "message": "Rate limit exceeded"
                                }
                            ]
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"API请求失败: {response.status} - {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"错误: {str(e)}")
            return None

# 创建全局 API 实例
api = TwitterAPI(api_key="2e78k9hg7j2me2g1vhky7a5bh5r0r1")

async def search_tweets(search_content):
    """
    供 Discord 机器人调用的搜索函数
    """
    try:
        result = await api.search_tweets(query=search_content)
        return result
    except Exception as e:
        logger.error(f"搜索推文时出错: {str(e)}")
        return None

async def test():
    # 只测试一个查询
    query = "eth"
    logger.info("开始测试API...")
    logger.info(f"测试查询: {query}")
    
    try:
        # 直接使用 aiohttp 测试
        async with aiohttp.ClientSession() as session:
            # 使用与 curl 相同的参数格式
            params = {
                "q": query,
                "cursor": "",
                "sort_by": "Latest"
            }
            
            url = "https://api.apidance.pro/sapi/Search"
            headers = {
                "apikey": "2e78k9hg7j2me2g1vhky7a5bh5r0r1",
                "Content-Type": "application/json"
            }
            
            logger.info(f"请求URL: {url}")
            logger.info(f"请求参数: {params}")
            logger.info(f"请求头: {headers}")
            
            async with session.get(url, params=params, headers=headers, ssl=False) as response:
                logger.info(f"响应状态码: {response.status}")
                text = await response.text()
                logger.info(f"原始响应: {text}")
                
                if response.status == 200:
                    try:
                        data = json.loads(text)
                        logger.info(f"解析后的数据: {json.dumps(data, indent=2, ensure_ascii=False)}")
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON解析失败: {e}")
                else:
                    logger.error(f"请求失败: {response.status}")
                    
    except Exception as e:
        logger.error(f"发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test()) 