#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 在这里设置你的 API key
API_KEY = "CG-9gjYh6R3EBCbHCzeAxYG6zaC"

import aiohttp
import logging
from typing import Optional, Dict, List, Any, Union
import asyncio
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import time
import os
import socket
import ssl
import json

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoinGeckoAPI:
    """CoinGecko API 客户端类"""
    
    def __init__(self, api_key: str, base_url: str = "https://pro-api.coingecko.com/api/v3"):
        """
        初始化 CoinGecko API 客户端
        
        参数:
            api_key: CoinGecko API 密钥
            base_url: API 基础 URL
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": api_key
        }
    
    def check_connection(self, verbose: bool = True) -> Dict[str, bool]:
        """
        测试与 CoinGecko API 的网络连接
        
        参数:
            verbose: 是否打印详细信息
            
        返回:
            包含各项连接测试结果的字典
        """
        if verbose:
            print("正在进行网络连接测试...")
        
        results = {
            "dns": False,
            "tcp": False,
            "ssl": False
        }
        
        # 测试DNS解析
        try:
            hostname = self.base_url.replace("https://", "").replace("http://", "").split("/")[0]
            ip = socket.gethostbyname(hostname)
            results["dns"] = True
            if verbose:
                print(f"DNS解析成功: {hostname} -> {ip}")
        except Exception as e:
            if verbose:
                print(f"DNS解析失败: {e}")
        
        # 测试TCP连接
        try:
            sock = socket.create_connection((hostname, 443), timeout=10)
            sock.close()
            results["tcp"] = True
            if verbose:
                print("TCP连接成功")
        except Exception as e:
            if verbose:
                print(f"TCP连接失败: {e}")
        
        # 测试SSL连接
        try:
            context = ssl.create_default_context()
            with socket.create_connection((hostname, 443), timeout=10) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    results["ssl"] = True
                    if verbose:
                        print(f"SSL连接成功，使用{ssock.version()}")
        except Exception as e:
            if verbose:
                print(f"SSL连接失败: {e}")
                
        return results
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, 
                     max_retries: int = 3, timeout: int = 30) -> Dict[str, Any]:
        """
        发送请求到 CoinGecko API
        
        参数:
            endpoint: API 端点
            params: 请求参数
            max_retries: 最大重试次数
            timeout: 请求超时时间(秒)
            
        返回:
            API 响应数据
        """
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=timeout
                )
                
                # 检查响应状态
                if response.status_code == 200:
                    return response.json()
                else:
                    # 如果达到最大重试次数，抛出异常
                    if attempt == max_retries - 1:
                        raise Exception(f"API请求失败: 状态码 {response.status_code}, 响应: {response.text}")
                    # 429 表示请求过多，等待后重试
                    if response.status_code == 429:
                        time.sleep(2 ** attempt)  # 指数退避
                        continue
            except requests.exceptions.RequestException as e:
                # 如果达到最大重试次数，抛出异常
                if attempt == max_retries - 1:
                    raise Exception(f"请求异常: {str(e)}")
                time.sleep(1)  # 等待后重试
        
        # 如果所有重试都失败
        raise Exception("达到最大重试次数，请求失败")
    
    def get_token_info(self, network: str, token_address: str, include_pools: bool = True) -> Dict[str, Any]:
        """
        获取代币信息
        
        参数:
            network: 区块链网络 (如 'solana', 'eth' 等)
            token_address: 代币地址
            include_pools: 是否包含池信息
            
        返回:
            代币信息
        """
        endpoint = f"/onchain/networks/{network}/tokens/{token_address}"
        
        # 添加参数以请求包含池信息 - 修正参数格式
        params = {}
        if include_pools:
            params['include'] = 'top_pools'  # 正确的参数是'include=top_pools'
        
        return self._make_request(endpoint, params)
    
    def get_token_price(self, ids: Union[str, list], vs_currencies: Union[str, list] = 'usd') -> Dict[str, Any]:
        """
        获取代币价格
        
        参数:
            ids: 代币ID或ID列表
            vs_currencies: 计价货币或货币列表
            
        返回:
            代币价格信息
        """
        if isinstance(ids, list):
            ids = ','.join(ids)
        if isinstance(vs_currencies, list):
            vs_currencies = ','.join(vs_currencies)
            
        endpoint = "/simple/price"
        params = {
            'ids': ids,
            'vs_currencies': vs_currencies
        }
        
        return self._make_request(endpoint, params)
    
    def get_trending(self) -> Dict[str, Any]:
        """
        获取热门代币
        
        返回:
            热门代币信息
        """
        endpoint = "/search/trending"
        return self._make_request(endpoint)

    async def get_token_pools(self, network: str, token_address: str) -> Dict:
        """
        获取特定网络上代币的流动性池信息
        
        参数:
            network (str): 网络名称，如 'ethereum', 'binance-smart-chain' 等
            token_address (str): 代币合约地址
            
        返回:
            Dict: 包含代币池信息的字典
        """
        endpoint = f"/onchain/dex/networks/{network}/tokens/{token_address}/pools"
        logger.info(f"正在获取 {network} 网络上代币 {token_address} 的池信息")
        
        return await self._make_request(endpoint)

    @staticmethod
    def get_supported_networks() -> List[str]:
        """获取支持的网络列表"""
        return [
            "eth",
            "bsc",
            "polygon-pos",
            "fantom",
            "avalanche",
            "arbitrum-one",
            "optimism",
            "solana",
            "base",
            "kava"
        ]

    def is_supported_network(self, network: str) -> bool:
        """检查网络是否支持"""
        return network.lower() in self.get_supported_networks()

def clean_text(text, max_length=32000):
    """
    清理文本数据，移除特殊字符并限制长度
    """
    if not isinstance(text, str):
        return ''
    # 移除可能导致Excel保存问题的字符
    text = text.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    # 限制文本长度
    return text[:max_length] if len(text) > max_length else text

def clean_list(lst, max_length=1000):
    """
    清理列表数据，将其转换为字符串并限制长度
    """
    if not lst:
        return ''
    text = ', '.join(str(item) for item in lst if item)
    return clean_text(text, max_length)

def detect_network(token_address: str) -> list:
    """
    根据地址格式确定要尝试的网络列表
    
    参数:
        token_address: 代币地址
        
    返回:
        网络名称列表，按优先级排序
    """
    # 清理地址
    address = token_address.strip()
    
    # 以太坊系地址识别 (0x开头)
    if address.startswith('0x'):
        # 使用正确的网络参数: bsc 而不是 binance-smart-chain
        return ["bsc", "base", "eth"]
    
    # 非0x开头的地址，只尝试Solana网络
    return ["solana"]

async def try_multiple_networks(client, token_address: str) -> tuple:
    """
    根据地址格式，在适合的网络上尝试获取代币信息
    
    参数:
        client: API客户端实例
        token_address: 代币地址
        
    返回:
        (token_info, network) 元组，如果查询失败则返回(None, None)
    """
    # 获取要尝试的网络列表（根据地址格式确定）
    networks_to_try = detect_network(token_address)
    
    # 只尝试检测到的特定网络，不再尝试所有支持的网络
    for network in networks_to_try:
        try:
            logger.info(f"尝试在 {network} 网络上查询代币 {token_address}")
            token_info = client.get_token_info(network, token_address)
            
            # 检查是否成功获取数据
            if token_info and 'data' in token_info and 'attributes' in token_info['data']:
                logger.info(f"在 {network} 网络上成功找到代币 {token_address}")
                return token_info, network
        except Exception as e:
            logger.warning(f"在 {network} 网络上查询代币 {token_address} 失败: {str(e)}")
            continue
    
    # 指定的网络都查询失败，不再尝试其他网络
    if token_address.startswith('0x'):
        logger.error(f"地址 {token_address} 在BSC、Base和Ethereum上均未找到")
    else:
        logger.error(f"地址 {token_address} 在Solana网络上未找到")
    
    return None, None

async def process_token_info(start_index=0, batch_size=10):
    """
    批量处理Excel文件中的代币地址并获取信息，专注于交易相关数据
    """
    try:
        # 获取data目录路径
        data_dir = os.path.join(os.getcwd(), 'data')
        
        # 直接读取meme.xlsx文件
        input_file_path = os.path.join(data_dir, 'meme.xlsx')
        if not os.path.exists(input_file_path):
            logger.error("未找到meme.xlsx文件")
            return
            
        # 获取当前时间戳
        current_time = time.strftime('%Y%m%d_%H%M%S')
        output_excel_path = os.path.join(data_dir, f'token_trading_data_{current_time}.xlsx')
        
        # 读取Excel文件
        logger.info(f"正在读取文件: {input_file_path}")
        df = pd.read_excel(input_file_path)
        
        logger.info("Excel文件中的列名: %s", df.columns.tolist())
        logger.info(f"总行数: {len(df)}")
        logger.info(f"将从第 {start_index} 个代币开始处理...")
        
        # 添加交易相关的新列
        if 'token_id' not in df.columns:
            df['token_id'] = ''
        if 'address' not in df.columns:
            df['address'] = ''
        if 'symbol' not in df.columns:
            df['symbol'] = ''
        if 'fdv_usd' not in df.columns:
            df['fdv_usd'] = ''
        if 'volume_usd_24h' not in df.columns:
            df['volume_usd_24h'] = ''
        if 'price_change_24h' not in df.columns:
            df['price_change_24h'] = ''
        if 'buys_24h' not in df.columns:
            df['buys_24h'] = ''
        if 'sells_24h' not in df.columns:
            df['sells_24h'] = ''
        if 'network' not in df.columns:
            df['network'] = ''
        
        # 添加价格变化的新列
        if 'price_change_m5' not in df.columns:
            df['price_change_m5'] = ''
        if 'price_change_h1' not in df.columns:
            df['price_change_h1'] = ''
        
        # 添加交易数据的新列
        if 'm5_buys' not in df.columns:
            df['m5_buys'] = ''
        if 'm5_sells' not in df.columns:
            df['m5_sells'] = ''
        if 'm15_buys' not in df.columns:
            df['m15_buys'] = ''
        if 'm15_sells' not in df.columns:
            df['m15_sells'] = ''
        
        # 添加池创建时间列
        if 'pool_created_at' not in df.columns:
            df['pool_created_at'] = ''
        
        # 添加格式化列
        if 'fdv_usd_formatted' not in df.columns:
            df['fdv_usd_formatted'] = ''
        if 'volume_usd_24h_formatted' not in df.columns:
            df['volume_usd_24h_formatted'] = ''
        if 'price_change_m5_formatted' not in df.columns:
            df['price_change_m5_formatted'] = ''
        if 'price_change_h1_formatted' not in df.columns:
            df['price_change_h1_formatted'] = ''
        
        # 初始化API客户端
        client = CoinGeckoAPI(API_KEY)
        
        # 用于记录上次保存的时间
        last_save_time = time.time()
        modified = False
        
        # 统计计数器
        processed_count = 0
        success_count = 0
        error_count = 0
        
        # 遍历每个代币
        for index, row in df.iloc[start_index:].iterrows():
            try:
                token_address = row['内容']
                if pd.isna(token_address):
                    logger.warning(f"跳过空地址，索引 {index}")
                    continue
                
                processed_count += 1
                logger.info(f"正在获取索引 {index} ({processed_count}/{len(df)}) 的交易数据...")
                
                # 清理和验证token地址
                if isinstance(token_address, str):
                    token_address = token_address.strip()
                    # 宽松的验证，允许不同网络的地址格式
                    if len(token_address) < 20:
                        logger.warning(f"跳过无效的地址格式: {token_address}")
                        continue
                
                # 尝试多网络请求
                token_info, network = await try_multiple_networks(client, token_address)
                
                if token_info and network:
                    # 保存找到的网络
                    df.at[index, 'network'] = network
                    
                    # 提取交易相关数据
                    attributes = token_info['data']['attributes']
                    
                    # 基本信息
                    df.at[index, 'token_id'] = token_info['data'].get('id', '')
                    df.at[index, 'address'] = attributes.get('address', '')
                    df.at[index, 'symbol'] = attributes.get('symbol', '')
                    
                    # 市值和交易量
                    df.at[index, 'fdv_usd'] = attributes.get('fdv_usd', '')
                    df.at[index, 'fdv_usd_formatted'] = format_currency(attributes.get('fdv_usd', ''))
                    
                    # 24小时交易量
                    if 'volume_usd' in attributes and 'h24' in attributes['volume_usd']:
                        volume_24h = attributes['volume_usd']['h24']
                        df.at[index, 'volume_usd_24h'] = volume_24h
                        df.at[index, 'volume_usd_24h_formatted'] = format_currency(volume_24h)
                    
                    # 检查是否存在included数据（池信息）
                    if 'included' in token_info and token_info['included'] and len(token_info['included']) > 0:
                        # 提取第一个池的数据
                        pool = token_info['included'][0]
                        if 'attributes' in pool:
                            pool_attrs = pool['attributes']
                            
                            # 添加池创建时间
                            if 'pool_created_at' in pool_attrs:
                                utc_time = pool_attrs['pool_created_at']
                                df.at[index, 'pool_created_at'] = convert_utc_to_utc8(utc_time)
                            
                            # 价格变动 - 添加更多时间段
                            if 'price_change_percentage' in pool_attrs:
                                price_changes = pool_attrs['price_change_percentage']
                                # 5分钟价格变化
                                if 'm5' in price_changes:
                                    m5_change = price_changes['m5']
                                    df.at[index, 'price_change_m5'] = m5_change
                                    df.at[index, 'price_change_m5_formatted'] = format_percentage(m5_change)
                                # 1小时价格变化
                                if 'h1' in price_changes:
                                    h1_change = price_changes['h1']
                                    df.at[index, 'price_change_h1'] = h1_change
                                    df.at[index, 'price_change_h1_formatted'] = format_percentage(h1_change)
                            else:
                                logger.warning(f"未找到池 {pool.get('id', '未知')} 的价格变动数据")
                            
                            # 交易数量 - 使用m5和m15，删除h24
                            if 'transactions' in pool_attrs:
                                txs = pool_attrs['transactions']
                                # 5分钟交易
                                if 'm5' in txs:
                                    df.at[index, 'm5_buys'] = txs['m5'].get('buys', 0)
                                    df.at[index, 'm5_sells'] = txs['m5'].get('sells', 0)
                                # 15分钟交易
                                if 'm15' in txs:
                                    df.at[index, 'm15_buys'] = txs['m15'].get('buys', 0)
                                    df.at[index, 'm15_sells'] = txs['m15'].get('sells', 0)
                            else:
                                logger.warning(f"未找到池 {pool.get('id', '未知')} 的交易数量数据")
                    else:
                        logger.warning(f"代币 {token_address} 未找到池信息，无法获取价格变动和交易数据")
                    
                    modified = True
                    success_count += 1
                    logger.info(f"已保存代币 {token_address} 的交易数据")
                else:
                    error_count += 1
                    logger.error(f"在所有支持的网络上都无法获取 {token_address} 的交易数据")
                
                # 每处理batch_size条数据或者经过60秒就保存一次
                current_time = time.time()
                if modified and (processed_count % batch_size == 0 or current_time - last_save_time >= 60):
                    try:
                        logger.info(f"准备保存进度，已处理 {processed_count} 条数据...")
                        temp_file = output_excel_path.replace('.xlsx', '_temp.xlsx')
                        df.to_excel(temp_file, index=False)
                        if os.path.exists(output_excel_path):
                            os.remove(output_excel_path)
                        os.rename(temp_file, output_excel_path)
                        logger.info(f"已保存当前进度到: {output_excel_path}")
                        last_save_time = current_time
                        modified = False
                    except Exception as save_error:
                        logger.error(f"保存文件时出错: {str(save_error)}")
                
                time.sleep(1)  # 添加延迟以避免触发API限制
                
            except Exception as e:
                error_count += 1
                logger.error(f"处理代币 {token_address} 时出错: {str(e)}")
                continue
        
        # 最后保存一次
        if modified:
            try:
                temp_file = output_excel_path.replace('.xlsx', '_temp.xlsx')
                df.to_excel(temp_file, index=False)
                if os.path.exists(output_excel_path):
                    os.remove(output_excel_path)
                os.rename(temp_file, output_excel_path)
                logger.info("最终交易数据已保存")
            except Exception as final_save_error:
                logger.error(f"最终保存文件时出错: {str(final_save_error)}")
        
        # 打印统计信息
        logger.info("\n处理统计信息:")
        logger.info(f"总记录数: {len(df)}")
        logger.info(f"处理记录数: {processed_count}")
        logger.info(f"成功处理数: {success_count}")
        logger.info(f"失败记录数: {error_count}")
        
    except Exception as e:
        logger.error(f"处理过程中出现错误: {str(e)}")

async def main():
    """测试代码"""
    # 使用直接定义的 API_KEY
    api_key = API_KEY
    
    if not api_key:
        logger.error("API密钥未设置")
        return
    
    # 创建API实例
    api = CoinGeckoAPI(api_key)
    
    # 测试获取代币池信息（使用Solana测试代币）
    test_token = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC on Solana
    result = await api.get_token_pools("solana", test_token)
    
    if "error" not in result:
        logger.info(f"成功获取池信息: {len(result.get('pools', []))} 个池")
        # 打印第一个池的信息作为示例
        if result.get('pools'):
            logger.info(f"示例池信息: {result['pools'][0]}")
    else:
        logger.error(f"获取池信息失败: {result['error']}")

def format_percentage(value):
    """
    将数值格式化为百分比形式
    
    参数:
        value: 数值
        
    返回:
        格式化后的百分比字符串，如 "5.2%"
    """
    if pd.isna(value) or value == '':
        return ''
    try:
        # 尝试转换为浮点数
        num_value = float(value)
        return f"{num_value}%"
    except (ValueError, TypeError):
        return value

def format_currency(value, decimal_places=2):
    """
    将货币数值格式化为更易读的形式 (K, M, B)
    
    参数:
        value: 货币数值
        decimal_places: 小数位数
        
    返回:
        格式化后的货币字符串，如 "5.2M"
    """
    if pd.isna(value) or value == '':
        return ''
    
    try:
        # 尝试转换为浮点数
        num_value = float(value)
        
        # 根据数值大小选择合适的单位
        if abs(num_value) >= 1_000_000_000:  # 十亿
            return f"{num_value / 1_000_000_000:.{decimal_places}f}B"
        elif abs(num_value) >= 1_000_000:  # 百万
            return f"{num_value / 1_000_000:.{decimal_places}f}M"
        elif abs(num_value) >= 1_000:  # 千
            return f"{num_value / 1_000:.{decimal_places}f}K"
        else:
            return f"{num_value:.{decimal_places}f}"
    except (ValueError, TypeError):
        return value

def convert_utc_to_utc8(utc_time_str):
    """
    将UTC时间字符串转换为UTC+8时间字符串
    
    参数:
        utc_time_str: UTC时间字符串，如 "2022-07-13T11:48:12Z"
        
    返回:
        UTC+8时间字符串，如 "2022-07-13 19:48:12"
    """
    if not utc_time_str or pd.isna(utc_time_str):
        return ''
    
    try:
        # 解析UTC时间字符串
        # 处理带Z结尾的ISO格式或其他格式
        if 'Z' in utc_time_str:
            # 2022-07-13T11:48:12Z 格式
            dt = datetime.strptime(utc_time_str.replace('Z', '+00:00'), "%Y-%m-%dT%H:%M:%S%z")
        elif '+' in utc_time_str or '-' in utc_time_str[-6:]:
            # 已经包含时区信息的格式
            dt = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S%z")
        else:
            # 没有时区信息的格式，假设为UTC
            dt = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S")
            # 将其设置为UTC时区
            dt = dt.replace(tzinfo=timezone.utc)
        
        # 转换到UTC+8
        dt_utc8 = dt + timedelta(hours=8)
        
        # 格式化为易读的字符串格式
        return dt_utc8.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning(f"时间转换失败: {str(e)}, 原始时间: {utc_time_str}")
        return utc_time_str  # 如果转换失败，返回原始时间字符串

def get_token_data(token_id):
    """
    从CoinGecko获取代币详细信息
    
    参数:
        token_id (str): 代币ID或地址
        
    返回:
        dict: 包含代币数据的字典，获取失败则返回None
    """
    # 检查token_id是否为空
    if not token_id or token_id.strip() == "":
        logger.error("代币ID为空，无法获取CoinGecko数据")
        return None
        
    logger.info(f"开始从CoinGecko获取代币数据: {token_id}")
    
    # 添加API密钥
    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": API_KEY
    }
    
    for attempt in range(1, 4):
        try:
            logger.info(f"正在获取代币 {token_id} 的数据 (尝试 {attempt}/3)")
            # 确保token_id被正确添加到URL中
            url = f"https://pro-api.coingecko.com/api/v3/coins/{token_id}"
            logger.info(f"请求CoinGecko API: {url}")
            
            # 发送请求
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"成功获取代币 {token_id} 的数据")
                
                # 整理代币数据
                coin_data = {
                    'symbol': data.get('symbol', '').upper(),
                    'name': data.get('name', ''),
                    'token_id': data.get('id', ''),
                    'fdv_usd': data.get('market_data', {}).get('fully_diluted_valuation', {}).get('usd', 0),
                    'volume_usd_24h': data.get('market_data', {}).get('total_volume', {}).get('usd', 0),
                    'current_price_usd': data.get('market_data', {}).get('current_price', {}).get('usd', 0)
                }
                
                return coin_data
            elif response.status_code == 429:
                logger.warning(f"API速率限制，等待后重试")
                time.sleep(2 ** attempt)  # 指数退避
            else:
                logger.error(f"API请求失败: 状态码 {response.status_code}")
                logger.error(f"响应内容: {response.text}")
                if attempt < 3:
                    time.sleep(1)
                
        except Exception as e:
            logger.error(f"获取代币数据时出错: {str(e)}")
            if attempt < 3:
                time.sleep(1)
    
    logger.error(f"无法获取代币 {token_id} 的数据，已达到最大重试次数")
    return None

if __name__ == "__main__":
    # asyncio.run(main())
    # asyncio.run(process_token_info())  # 改为调用process_token_info函数
    
    # 在Windows上设置事件循环策略
    if os.name == 'nt':  # Windows系统
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(process_token_info())