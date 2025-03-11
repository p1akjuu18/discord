import asyncio
from tweet_metrics import TweetMetrics

async def test_single_tweet():
    api_key = "2e78k9hg7j2me2g1vhky7a5bh5r0r1"
    tweet_metrics = TweetMetrics(api_key)
    
    # 测试URL
    test_url = "https://x.com/dxrnelljcl/status/1895126881881444645"
    
    # 提取ID
    tweet_id = tweet_metrics.extract_tweet_id(test_url)
    print(f"提取的推文ID: {tweet_id}")
    
    # 获取数据
    metrics = await tweet_metrics.get_tweet_metrics(test_url)
    if metrics:
        print("\n推文互动数据:")
        print(f"点赞数: {metrics['likes']}")
        print(f"转发数: {metrics['retweets']}")
        print(f"评论数: {metrics['replies']}")
        print(f"引用数: {metrics['quotes']}")
    else:
        print("获取推文数据失败")

if __name__ == "__main__":
    asyncio.run(test_single_tweet()) 