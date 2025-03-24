import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_session_with_retries():
    """创建带有重试机制的会话"""
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=5,  # 最大重试次数
        backoff_factor=1,  # 重试间隔
        status_forcelist=[500, 502, 503, 504],  # 需要重试的HTTP状态码
    )
    
    # 创建适配器
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    # 将适配器应用到http和https
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_historical_klines(symbol, interval, start_str, end_str=None):
    """获取历史K线数据"""
    
    endpoint = "https://api.binance.com/api/v3/klines"
    
    # 转换日期字符串为时间戳
    start_ts = int(datetime.strptime(start_str, '%Y-%m-%d').timestamp() * 1000)
    if end_str:
        end_ts = int(datetime.strptime(end_str, '%Y-%m-%d').timestamp() * 1000)
    else:
        end_ts = int(time.time() * 1000)
    
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": 1000
    }
    
    try:
        # 使用带重试机制的会话
        session = create_session_with_retries()
        
        # 发送请求时禁用SSL验证
        response = session.get(endpoint, params=params, verify=False)
        response.raise_for_status()
        
        # 处理响应数据
        data = response.json()
        
        # 转换为DataFrame
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                       'close_time', 'quote_asset_volume', 'number_of_trades',
                                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                       'ignore'])
        
        # 转换时间戳
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 转换数值类型
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"获取数据时出错: {str(e)}")
        return None
    finally:
        session.close()

# 添加警告过滤
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 使用示例
if __name__ == "__main__":
    # 获取2025年3月1日至3月15日的BTC/USDT每小时K线数据
    df = get_historical_klines('BTCUSDT', '1h', '2025-03-01', '2025-03-15')
    
    if not df.empty:
        print(f"获取到 {len(df)} 条历史数据")
        print(df.head())
        
        # 保存到CSV文件
        df.to_csv('btcusdt_history.csv', index=False)
        print("数据已保存到 btcusdt_history.csv")
    else:
        print("未获取到数据") 