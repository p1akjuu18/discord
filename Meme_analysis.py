#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import json
import sys
import logging
import aiohttp
from datetime import datetime, timedelta
import os
from pathlib import Path
import pandas as pd
import time
import threading
from typing import Optional, Dict, List, Any
import re
import twitter_api
import hmac
import hashlib
import base64
import requests
import traceback
from feishu_bot import FeishuBot
import lark_oapi as lark
from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
import coingecko_api 
import argparse
# 添加 watchdog 导入
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

__all__ = ['MemeAnalyzer', 'BacktestProcessor', 'process_message', 'CoinGeckoAnalyzer', 'MemeFileWatcher']  # 新增 MemeFileWatcher

# 添加 TelegramBot 类 - 移到前面定义
class TelegramBot:
    """处理 Telegram 消息发送功能"""
    
    def __init__(self, token=None):
        """
        初始化 Telegram 机器人
        
        参数:
            token: Telegram Bot API 令牌
        """
        # 从配置文件加载，如果未提供
        if token is None:
            with open('config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                token = config.get('telegram_token', '')
                
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        
        logger.info(f"Telegram机器人初始化，Token: {token[:6]}...{token[-4:] if token else ''}")
    
    def send_message(self, chat_id, text, parse_mode='Markdown'):
        """
        发送消息到 Telegram
        
        参数:
            chat_id: 目标聊天ID或聊天ID列表
            text: 消息内容
            parse_mode: 解析模式，可选 'Markdown' 或 'HTML'
            
        返回:
            成功返回 True，失败返回 False
        """
        try:
            # 将单个聊天ID转换为列表
            if isinstance(chat_id, str):
                chat_ids = [chat_id]
            else:
                chat_ids = chat_id
            
            success = False
            
            # 发送到每个聊天ID
            for single_chat_id in chat_ids:
                url = f"{self.base_url}/sendMessage"
                payload = {
                    'chat_id': single_chat_id,
                    'text': text,
                    'parse_mode': parse_mode
                }
                
                response = requests.post(url, json=payload)
                
                if response.status_code == 200:
                    logger.info(f"消息已成功发送到 Telegram 聊天 {single_chat_id}")
                    success = True
                else:
                    logger.error(f"发送 Telegram 消息失败: {response.status_code} - {response.text}")
            
            return success
                
        except Exception as e:
            logger.error(f"发送 Telegram 消息时出错: {str(e)}")
            logger.error(traceback.format_exc())
            return False

# 首先定义 AnalysisIntegrator 类
class AnalysisIntegrator:
    """整合多个数据源的分析结果"""
    
    def __init__(self, app_id=None, app_secret=None):
        # 加载配置
        with open('config.json', 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        # 飞书配置
        self.app_id = app_id
        self.app_secret = app_secret
        
        # 只有当提供了app_id和app_secret时才初始化飞书机器人
        if app_id and app_secret:
            self.feishu_bot = FeishuBot(app_id=app_id, app_secret=app_secret)
        else:
            self.feishu_bot = None
        
        # 飞书聊天ID
        self.feishu_chat_id = self.config.get('feishu_chat_id', 'oc_a2d2c5616c900bda2ab8e13a77361287')
        
        # 添加 Telegram 支持
        telegram_token = self.config.get('telegram_token', '')
        if telegram_token:
            self.telegram_bot = TelegramBot(token=telegram_token)
        else:
            self.telegram_bot = None
        
        self.telegram_chat_id = self.config.get('telegram_chat_id', '')
        
        self.data_dir = Path('data')
        self.analysis_path = self.data_dir / 'integrated_analysis_results.xlsx'
        self.pending_tokens = {}
        self.processed_tokens = set()
        self.sending_tokens = set()
        
        # 确保数据目录存在
        self._ensure_data_directory()
        
        # 创建或加载结果文件
        self._init_results_file()
    
    def _ensure_data_directory(self):
        """确保数据目录存在"""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"数据目录已确认: {self.data_dir}")
        except Exception as e:
            logger.error(f"创建数据目录失败: {str(e)}")
            raise
    
    def _init_results_file(self):
        """初始化结果文件"""
        if not self.analysis_path.exists():
            # 创建空的DataFrame并设置列
            columns = [
                '代币地址', '分析时间',
                # Twitter和Deepseek分析结果
                '搜索关键词', '叙事信息', '可持续性_社区热度', '可持续性_传播潜力', 
                '可持续性_短期投机价值', '原始推文数量',
                # CoinGecko数据
                'symbol', 'name', 'network', 'token_id',
                'fdv_usd', 'volume_usd_24h', 'price_change_m5', 'price_change_h1',
                'm5_buys', 'm5_sells', 'm15_buys', 'm15_sells', 'pool_created_at',
                # 标记字段
                'twitter_analyzed', 'coingecko_analyzed', 'sent_to_feishu'
            ]
            
            df = pd.DataFrame(columns=columns)
            df.to_excel(self.analysis_path, index=False)
            logger.info(f"创建了新的整合分析结果文件: {self.analysis_path}")
    
    def register_token(self, token_address):
        """
        注册一个新的代币分析任务
        
        参数:
            token_address: 代币地址
        
        返回:
            任务ID
        """
        if token_address in self.pending_tokens:
            logger.info(f"代币 {token_address} 已在处理队列中")
            return
            
        # 检查是否已处理过
        if token_address in self.processed_tokens:
            logger.info(f"代币 {token_address} 已处理过，跳过")
            return
            
        # 将代币添加到待处理列表
        self.pending_tokens[token_address] = {
            '代币地址': token_address,
            '分析时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'twitter_analyzed': False,
            'coingecko_analyzed': False,
            'sent_to_feishu': False
        }
        
        logger.info(f"注册了新的代币分析任务: {token_address}")
        
        # 将新任务保存到文件
        try:
            df = pd.read_excel(self.analysis_path)
            
            # 检查是否已存在
            if not ((df['代币地址'] == token_address) & (df['sent_to_feishu'] == False)).any():
                # 添加新行
                new_row = pd.DataFrame([self.pending_tokens[token_address]])
                df = pd.concat([df, new_row], ignore_index=True)
                df.to_excel(self.analysis_path, index=False)
                logger.info(f"已将代币 {token_address} 添加到分析结果文件")
        except Exception as e:
            logger.error(f"保存新任务时出错: {str(e)}")
    
    def update_twitter_analysis(self, token_address, analysis_result):
        """
        更新Twitter和Deepseek分析结果
        
        参数:
            token_address: 代币地址
            analysis_result: 分析结果字典
        """
        if token_address not in self.pending_tokens:
            self.register_token(token_address)
        
        # 更新内存中的分析结果
        token_data = self.pending_tokens[token_address]
        token_data.update({
            '搜索关键词': analysis_result.get('搜索关键词', ''),
            '叙事信息': analysis_result.get('叙事信息', ''),
            '可持续性_社区热度': analysis_result.get('可持续性_社区热度', ''),
            '可持续性_传播潜力': analysis_result.get('可持续性_传播潜力', ''),
            '可持续性_短期投机价值': analysis_result.get('可持续性_短期投机价值', ''),
            '原始推文数量': analysis_result.get('原始推文数量', 0),
            'twitter_analyzed': True
        })
        
        # 更新文件
        self._update_analysis_file(token_address, token_data)
        
        # 检查是否所有分析都完成
        self._check_and_send(token_address)
        
    def update_coingecko_analysis(self, token_address, coin_data):
        """
        更新CoinGecko分析结果
        
        参数:
            token_address: 代币地址
            coin_data: CoinGecko分析结果字典
        """
        if token_address not in self.pending_tokens:
            self.register_token(token_address)
        
        # 更新内存中的分析结果
        token_data = self.pending_tokens[token_address]
        token_data.update({
            'symbol': coin_data.get('symbol', ''),
            'name': coin_data.get('name', ''),
            'network': coin_data.get('network', ''),
            'token_id': coin_data.get('token_id', ''),
            'fdv_usd': coin_data.get('fdv_usd', ''),
            'volume_usd_24h': coin_data.get('volume_usd_24h', ''),
            'price_change_m5': coin_data.get('price_change_m5', ''),
            'price_change_h1': coin_data.get('price_change_h1', ''),
            'm5_buys': coin_data.get('m5_buys', 0),
            'm5_sells': coin_data.get('m5_sells', 0),
            'm15_buys': coin_data.get('m15_buys', 0),
            'm15_sells': coin_data.get('m15_sells', 0),
            'pool_created_at': coin_data.get('pool_created_at', ''),
            'coingecko_analyzed': True
        })
        
        # 更新文件
        self._update_analysis_file(token_address, token_data)
        
        # 检查是否所有分析都完成
        self._check_and_send(token_address)
    
    def _update_analysis_file(self, token_address, token_data):
        """使用临时文件更新分析结果"""
        try:
            # 创建临时文件
            temp_file = self.analysis_path.with_suffix('.tmp')
            
            # 读取现有数据
            if self.analysis_path.exists():
                df = pd.read_excel(self.analysis_path)
            else:
                df = pd.DataFrame(columns=self._get_columns())
            
            # 更新数据
            mask = df['代币地址'] == token_address
            if mask.any():
                for col, value in token_data.items():
                    if col in df.columns:
                        df.loc[mask, col] = value
            else:
                new_row = pd.DataFrame([token_data])
                df = pd.concat([df, new_row], ignore_index=True)
            
            # 保存到临时文件
            df.to_excel(temp_file, index=False)
            
            # 替换原文件
            if self.analysis_path.exists():
                self.analysis_path.unlink()
            temp_file.rename(self.analysis_path)
            
            logger.info(f"已更新代币 {token_address} 的分析结果")
            
        except Exception as e:
            logger.error(f"更新分析文件时出错: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def _check_and_send(self, token_address):
        """检查是否所有分析都完成，如果是则发送到飞书"""
        token_data = self.pending_tokens.get(token_address)
        
        if not token_data:
            return
        
        # 检查是否正在发送中
        if token_address in self.sending_tokens:
            return
        
        # 检查是否需要两种分析都完成
        need_both = self._need_both_analyses()
        
        # 关键修改: 检查是否已经发送过或者条件不满足
        if token_data.get('sent_to_feishu'):
            return  # 如果已经发送过，直接返回，不重复发送
        
        # 如果需要两种分析都完成，但其中一个未完成，则返回
        if need_both and not (token_data.get('twitter_analyzed') and token_data.get('coingecko_analyzed')):
            return
        
        # 如果代码执行到这里，意味着可以发送消息了
        # 添加发送中标志
        self.sending_tokens.add(token_address)
        
        try:
            # 发送消息
            success = self._send_integrated_analysis(token_address)
            
            if success:
                # 更新状态
                token_data['sent_to_feishu'] = True
                self._update_analysis_file(token_address, token_data)
                
                # 添加到已处理集合
                self.processed_tokens.add(token_address)
                
                # 清理内存中的数据
                if token_address in self.pending_tokens:
                    del self.pending_tokens[token_address]
        finally:
            # 移除发送中标志
            self.sending_tokens.remove(token_address)
    
    def _need_both_analyses(self):
        """确定是否需要同时完成Twitter和CoinGecko分析"""
        # 这里可以根据配置或命令行参数决定是否需要两种分析都完成
        # 默认需要两种分析都完成
        return True
    
    def _send_integrated_analysis(self, token_address):
        """发送整合后的分析结果到飞书和Telegram"""
        try:
            token_data = self.pending_tokens.get(token_address)
            
            if not token_data:
                logger.error(f"找不到代币 {token_address} 的分析数据")
                return False
            
            # 构建消息
            message = f"""🔍金狗预警

📌 代币地址: {token_address}"""

            # 添加 CoinGecko 数据
            message += f"""
🪙 名称: {token_data.get('name', 'N/A')} ({token_data.get('symbol', 'N/A')})
🌐 网络: {token_data.get('network', 'N/A')}

💰 市场数据:
• 市值: {coingecko_api.format_currency(token_data.get('fdv_usd', 'N/A'))}
• 24小时交易量: {coingecko_api.format_currency(token_data.get('volume_usd_24h', 'N/A'))}
• 创建时间: {token_data.get('pool_created_at', 'N/A')}

📈 价格变动:
• 5分钟: {coingecko_api.format_percentage(token_data.get('price_change_m5', 'N/A'))}
• 1小时: {coingecko_api.format_percentage(token_data.get('price_change_h1', 'N/A'))}

🔄 最近交易次数:
• 5分钟内: 买入 {token_data.get('m5_buys', 0)} 次, 卖出 {token_data.get('m5_sells', 0)} 次
• 15分钟内: 买入 {token_data.get('m15_buys', 0)} 次, 卖出 {token_data.get('m15_sells', 0)} 次"""

            # 添加 Twitter 分析数据
            message += f"""

📝 叙事信息:
{token_data.get('叙事信息', 'N/A')}

🌡️ 可持续性分析:
• 社区热度: {token_data.get('可持续性_社区热度', 'N/A')}
• 传播潜力: {token_data.get('可持续性_传播潜力', 'N/A')}
• 短期投机价值: {token_data.get('可持续性_短期投机价值', 'N/A')}"""

            # 发送到飞书
            feishu_success = True
            if self.feishu_bot and self.feishu_chat_id:
                feishu_success = self.feishu_bot.send_message(
                    receive_id=self.feishu_chat_id,
                    content=message,
                    use_webhook=False
                )
                
                if feishu_success:
                    logger.info(f"已成功发送代币 {token_address} 的综合分析结果到飞书")
                else:
                    logger.error(f"发送代币 {token_address} 的综合分析结果到飞书失败")
            
            # 发送到Telegram
            telegram_success = True
            if self.telegram_bot and self.telegram_chat_id:
                telegram_success = self.telegram_bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=message
                )
                
                if telegram_success:
                    logger.info(f"已成功发送代币 {token_address} 的综合分析结果到Telegram")
                else:
                    logger.error(f"发送代币 {token_address} 的综合分析结果到Telegram失败")
            
            # 如果至少一个平台发送成功，则返回成功
            return feishu_success or telegram_success
            
        except Exception as e:
            logger.error(f"发送综合分析结果时出错: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _save_with_retry(self, func, max_retries=3, delay=1):
        """带重试机制的保存操作"""
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"保存操作失败，{delay}秒后重试: {str(e)}")
                time.sleep(delay)

# 然后创建实例
# 从配置文件获取app_id和app_secret
import json

def load_config(config_file='config.json'):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {str(e)}")
        return {}

# 加载配置
config = load_config()
app_id = config.get('feishu_app_id', '')
app_secret = config.get('feishu_app_secret', '')

# 修改这一行，传入app_id和app_secret
integrator = AnalysisIntegrator(app_id=app_id, app_secret=app_secret)

# 然后是其他类定义
class MemeAnalyzer:
    def __init__(self, config_file='config.json', api_key=None):
        self.config = self.load_config(config_file)
        self.setup_directories()
        
        # API配置
        self.base_url = self.config.get("base_url", "https://api.siliconflow.cn")
        self.api_key = api_key or self.config.get("api_keys", {}).get("deepseek")
        
        # 验证 API key 格式
        if not self.api_key:
            logger.error("Deepseek API key未设置")
            raise ValueError("Deepseek API key is required")
        elif not self.api_key.startswith("sk-"):
            logger.error("Deepseek API key 格式错误，应该以 sk- 开头")
            raise ValueError("Invalid Deepseek API key format")
            
        logger.info(f"Deepseek API key 格式验证通过")
        
        self.min_occurrence_threshold = self.config.get("min_occurrence_threshold", 2)
        self.term_history = {}
        self.history_cleanup_threshold = timedelta(hours=self.config.get("history_cleanup_threshold", 24))
        
        # 初始化飞书机器人 - 传入正确的凭据
        self.app_id = "cli_a736cea2ff78100d"
        self.app_secret = "C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS"
        self.feishu_bot = FeishuBot(app_id=self.app_id, app_secret=self.app_secret)
        
        self.feishu_chat_id = self.config.get("feishu_chat_id", "oc_a2d2c5616c900bda2ab8e13a77361287")
        self.integrator = integrator

    def load_config(self, config_file):
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8-sig') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def setup_directories(self):
        """设置必要的目录"""
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # 设置Excel文件路径
        self.twitter_results_path = self.data_dir / 'twitter_results.xlsx'
        self.meme_path = self.data_dir / 'meme.xlsx'

    async def analyze_tweets(self, term: str, tweets: List[dict]) -> dict:
        """使用 Deepseek API 分析推文"""
        try:
            # 提取推文内容并清理
            tweet_texts = []
            for tweet in tweets:
                text = tweet.get('text', '').strip()
                if text:
                    # 清理合约地址
                    text = re.sub(r'[A-Za-z0-9]{32,}', '', text)
                    # 清理URL
                    text = re.sub(r'https?://\S+', '', text)
                    # 清理多余空白
                    text = ' '.join(text.split())
                    if text.strip():  # 确保清理后还有内容
                        tweet_texts.append(text)
            
            if not tweet_texts:
                logger.warning(f"清理后没有找到有效的推文内容用于分析")
                return self._get_default_analysis(term, len(tweets))
            
            # 去重
            tweet_texts = list(set(tweet_texts))
            
            # 修改认证头格式
            headers = {
                "Authorization": f"Bearer {self.api_key}",  # 确保是 Bearer 认证
                "Content-Type": "application/json",
                "Accept": "application/json"  # 添加 Accept 头
            }
            
            # 检查并记录 API key（隐藏部分内容）
            masked_key = f"{self.api_key[:6]}...{self.api_key[-4:]}" if self.api_key else "None"
            logger.info(f"使用的 API key: {masked_key}")
            
            data = {
                "model": "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B",
                "messages": [
                    {
                        "role": "user",
                        "content": f"""你是一个专业的加密货币分析师，我希望你能帮我评估这个 Meme 币的潜力，并给出详细的分析和建议，请分析以下关于加密货币的推文内容：

{chr(10).join(tweet_texts)}

请从以下两个方面分别进行分析，分2点，并用中文回答，我需要的结果不超过100字，你需要分以下2点明确的返回：

1. 叙事信息：用2-3句话总结这个meme币的核心和它的核心卖点。

2. 可持续性：从以下维度评估：
   - 社区热度
   - 传播潜力
   - 短期投机价值"""
                    }
                ],
                "stream": False,
                "temperature": 0.7,
                "max_tokens": 512,
                "top_p": 0.7,
                "top_k": 50,
                "frequency_penalty": 0.5
            }
            
            logger.info(f"发送Deepseek API请求，分析 {len(tweet_texts)} 条推文")
            
            max_retries = 3
            retry_delay = 10
            
            for attempt in range(max_retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{self.base_url}/v1/chat/completions",
                            headers=headers,
                            json=data,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as response:
                            response_text = await response.text()
                            logger.info(f"Deepseek API响应状态码: {response.status}")
                            logger.info(f"Deepseek API响应头: {dict(response.headers)}")
                            logger.info(f"Deepseek API请求数据: {json.dumps(data, ensure_ascii=False)}")
                            logger.info(f"Deepseek API响应内容: {response_text}")
                            
                            if response.status == 200:
                                result = json.loads(response_text)
                                if 'choices' in result and len(result['choices']) > 0:
                                    analysis = result['choices'][0]['message']['content']
                                    
                                    # 解析分析结果
                                    narrative = ""
                                    community_heat = ""
                                    spread_potential = ""
                                    investment_value = ""
                                    
                                    # 移除所有 Markdown 标记
                                    analysis = analysis.replace('**', '')
                                    
                                    # 分割主要部分
                                    parts = analysis.split('\n\n')
                                    
                                    # 解析叙事信息和可持续性评估
                                    for part in parts:
                                        if '1. 叙事信息' in part:
                                            narrative = part.replace('1. 叙事信息：', '').strip()
                                        elif '2. 可持续性' in part:
                                            lines = part.split('\n')
                                            for line in lines:
                                                line = line.strip()
                                                if '社区热度' in line:
                                                    community_heat = line.split('：')[1].strip() if '：' in line else ''
                                                elif '传播潜力' in line:
                                                    spread_potential = line.split('：')[1].strip() if '：' in line else ''
                                                elif '短期投机价值' in line:
                                                    investment_value = line.split('：')[1].strip() if '：' in line else ''
                                    
                                    result_dict = {
                                        '搜索关键词': term,
                                        '叙事信息': narrative,
                                        '可持续性_社区热度': community_heat,
                                        '可持续性_传播潜力': spread_potential,
                                        '可持续性_短期投机价值': investment_value,
                                        '原始推文数量': len(tweets),
                                        '分析时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    }
                                    
                                    logger.info(f"成功完成分析: {result_dict}")
                                    
                                    # 更新整合器的 Twitter 分析结果
                                    self.integrator.update_twitter_analysis(term, result_dict)
                                    
                                    return result_dict
                                    
                            elif response.status == 400:
                                logger.error(f"Deepseek API请求参数错误: {response_text}")
                                try:
                                    error_data = json.loads(response_text)
                                    logger.error(f"错误详情: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
                                except:
                                    logger.error(f"无法解析错误响应: {response_text}")
                                return self._get_default_analysis(term, len(tweets))
                            elif response.status == 401:
                                logger.error("Deepseek API认证失败，请检查API key")
                                return self._get_default_analysis(term, len(tweets))
                            elif response.status == 429:
                                logger.warning("Deepseek API速率限制")
                                return self._get_default_analysis(term, len(tweets))
                            else:
                                logger.error(f"Deepseek API请求失败: {response.status}")
                                logger.error(f"错误响应: {response_text}")
                                return self._get_default_analysis(term, len(tweets))
                                
                except Exception as e:
                    logger.error(f"调用Deepseek API时出错: {str(e)}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    
            return self._get_default_analysis(term, len(tweets))
            
        except Exception as e:
            logger.error(f"分析推文时发生错误: {str(e)}")
            logger.exception(e)
            return self._get_default_analysis(term, len(tweets))

    def _get_default_analysis(self, term: str, tweet_count: int) -> dict:
        """返回默认的分析结果"""
        return {
            '搜索关键词': term,
            '叙事信息': f'API认证失败，无法分析。共有{tweet_count}条推文',
            '可持续性_社区热度': '未知',
            '可持续性_传播潜力': '未知',
            '可持续性_短期投机价值': '未知',
            '原始推文数量': tweet_count,
            '分析时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    async def process_history_file(self):
        """处理历史数据文件"""
        try:
            logger.info("开始处理meme.xlsx历史数据")
            
            if not self.meme_path.exists():
                logger.error("meme.xlsx文件不存在")
                return
                
            df = pd.read_excel(self.meme_path)
            logger.info(f"加载了 {len(df)} 条历史记录")
            
            # 检查必要的列是否存在
            if '内容' not in df.columns:
                logger.error("meme.xlsx文件缺少'内容'列")
                return
                
            # 创建 CoinGecko 分析器实例
            coingecko_analyzer = CoinGeckoAnalyzer()
            
            # 处理每一行数据
            for _, row in df.iterrows():
                content = row['内容']
                logger.info(f"处理关键词: {content}")
                
                # 注册到整合器
                self.integrator.register_token(content)
                
                # 搜索Twitter
                tweets = await twitter_api.search_tweets(content)
                logger.info(f"找到 {len(tweets)} 条相关推文")
                
                if tweets:
                    # 分析推文
                    analysis = await self.analyze_tweets(content, tweets)
                    if analysis:
                        logger.info(f"已保存关键词 '{content}' 的分析结果")
                        
                        # 在这里调用 CoinGecko 分析
                        logger.info(f"开始对 '{content}' 进行 CoinGecko 分析...")
                        token_data = await coingecko_analyzer.analyze_token(content)
                        if token_data:
                            logger.info(f"完成对 '{content}' 的 CoinGecko 分析")
                        else:
                            logger.warning(f"CoinGecko 无法分析代币 '{content}'")
                    else:
                        logger.warning(f"关键词 '{content}' 的分析结果为空")
                else:
                    logger.warning(f"关键词 '{content}' 没有找到相关推文")
            
            logger.info("历史数据处理完成")
            
        except Exception as e:
            logger.error(f"处理历史数据文件时出错: {str(e)}")
            logger.exception(e)

class BacktestProcessor:
    def __init__(self):
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        self.meme_path = self.data_dir / 'meme.xlsx'
        self.twitter_results_path = self.data_dir / 'twitter_results.xlsx'
        
    async def save_meme_data(self, meme_data):
        """保存meme数据到Excel"""
        try:
            if self.meme_path.exists():
                df_meme = pd.read_excel(self.meme_path)
                df_meme = pd.concat([df_meme, pd.DataFrame(meme_data)], ignore_index=True)
            else:
                df_meme = pd.DataFrame(meme_data)
            
            df_meme.to_excel(self.meme_path, index=False)
            logger.info(f"成功保存 {len(meme_data)} 条meme数据到Excel")
        except Exception as e:
            logger.error(f"保存meme数据时出错: {e}")

# 创建全局实例
processor = BacktestProcessor()

async def process_message(message_data: Dict[str, Any]) -> None:
    """处理来自Discord的消息"""
    try:
        # 提取处理好的数据
        meme_data = message_data.get('meme_data', [])
        search_terms = message_data.get('search_terms', [])
        
        # 保存meme数据
        if meme_data:
            await processor.save_meme_data(meme_data)
            
    except Exception as e:
        logger.error(f"处理消息数据时出错: {str(e)}")

class CoinGeckoAnalyzer:
    """负责处理CoinGecko相关的代币分析功能"""
    
    def __init__(self, api_key=None):
        """
        初始化CoinGecko分析器
        
        参数:
            api_key: CoinGecko API 密钥，如果为None则使用模块默认值
        """
        self.api_key = api_key or coingecko_api.API_KEY
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        self.meme_path = self.data_dir / 'meme.xlsx'
        
        # 初始化API客户端
        self.client = coingecko_api.CoinGeckoAPI(self.api_key)
        
        # 添加整合器引用
        self.integrator = integrator
        
        logger.info("CoinGecko分析器初始化完成")
    
    async def analyze_token(self, token_address):
        """
        分析单个代币的交易数据
        
        参数:
            token_address: 代币地址
            
        返回:
            分析结果字典
        """
        try:
            logger.info(f"开始分析代币: {token_address}")
            
            # 清理和验证token地址
            if isinstance(token_address, str):
                token_address = token_address.strip()
                # 宽松的验证，允许不同网络的地址格式
                if len(token_address) < 20:
                    logger.warning(f"跳过无效的地址格式: {token_address}")
                    return None
            
            # 尝试多网络请求
            token_info, network = await coingecko_api.try_multiple_networks(self.client, token_address)
            
            if not token_info or not network:
                logger.warning(f"所有网络均未找到代币: {token_address}")
                return None
            
            # 提取基本信息和交易数据
            token_data = {
                'token_address': token_address,
                'network': network
            }
            
            # 提取详细属性
            attributes = token_info['data']['attributes']
            
            # 基本信息
            token_data['token_id'] = token_info['data'].get('id', '')
            token_data['address'] = attributes.get('address', '')
            token_data['symbol'] = attributes.get('symbol', '')
            token_data['name'] = attributes.get('name', '')
            
            # 市值和交易量
            token_data['fdv_usd'] = attributes.get('fdv_usd', '')
            token_data['fdv_usd_formatted'] = coingecko_api.format_currency(attributes.get('fdv_usd', ''))
            
            # 24小时交易量
            if 'volume_usd' in attributes and 'h24' in attributes['volume_usd']:
                volume_24h = attributes['volume_usd']['h24']
                token_data['volume_usd_24h'] = volume_24h
                token_data['volume_usd_24h_formatted'] = coingecko_api.format_currency(volume_24h)
            
            # 检查是否存在included数据（池信息）
            if 'included' in token_info and token_info['included'] and len(token_info['included']) > 0:
                # 提取第一个池的数据
                pool = token_info['included'][0]
                if 'attributes' in pool:
                    pool_attrs = pool['attributes']
                    
                    # 添加池创建时间
                    if 'pool_created_at' in pool_attrs:
                        utc_time = pool_attrs['pool_created_at']
                        token_data['pool_created_at'] = coingecko_api.convert_utc_to_utc8(utc_time)
                    
                    # 价格变动
                    if 'price_change_percentage' in pool_attrs:
                        price_changes = pool_attrs['price_change_percentage']
                        # 5分钟价格变化
                        if 'm5' in price_changes:
                            m5_change = price_changes['m5']
                            token_data['price_change_m5'] = m5_change
                            token_data['price_change_m5_formatted'] = coingecko_api.format_percentage(m5_change)
                        # 1小时价格变化
                        if 'h1' in price_changes:
                            h1_change = price_changes['h1']
                            token_data['price_change_h1'] = h1_change
                            token_data['price_change_h1_formatted'] = coingecko_api.format_percentage(h1_change)
                    
                    # 交易数量
                    if 'transactions' in pool_attrs:
                        txs = pool_attrs['transactions']
                        # 5分钟交易
                        if 'm5' in txs:
                            token_data['m5_buys'] = txs['m5'].get('buys', 0)
                            token_data['m5_sells'] = txs['m5'].get('sells', 0)
                        # 15分钟交易
                        if 'm15' in txs:
                            token_data['m15_buys'] = txs['m15'].get('buys', 0)
                            token_data['m15_sells'] = txs['m15'].get('sells', 0)
            
            logger.info(f"成功分析代币 {token_address} 的交易数据")
            
            # 在成功分析代币后，更新整合器的数据
            if token_data:
                self.integrator.update_coingecko_analysis(token_address, token_data)
            
            return token_data
            
        except Exception as e:
            logger.error(f"分析代币 {token_address} 时出错: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    async def send_token_analysis_to_feishu(self, token_data):
        """将代币分析结果发送到飞书"""
        if not token_data:
            logger.warning("无有效代币数据，跳过发送到飞书")
            return False
        
        try:
            # 创建适合飞书显示的消息格式
            message = f"""🪙 代币交易数据分析

📊 基本信息:
• 名称: {token_data.get('name', 'N/A')} ({token_data.get('symbol', 'N/A')})
• 网络: {token_data.get('network', 'N/A')}
• 地址: {token_data.get('address', 'N/A')}

💰 市场数据:
• 全面市值: {token_data.get('fdv_usd_formatted', 'N/A')}
• 24小时交易量: {token_data.get('volume_usd_24h_formatted', 'N/A')}

📈 价格变动:
• 5分钟: {token_data.get('price_change_m5_formatted', 'N/A')}
• 1小时: {token_data.get('price_change_h1_formatted', 'N/A')}

🔄 最近交易:
• 5分钟内: 买入 {token_data.get('m5_buys', 0)} 次, 卖出 {token_data.get('m5_sells', 0)} 次
• 15分钟内: 买入 {token_data.get('m15_buys', 0)} 次, 卖出 {token_data.get('m15_sells', 0)} 次

⏱️ 池创建时间: {token_data.get('pool_created_at', 'N/A')}"""

            success = self.feishu_bot.send_message(
                receive_id=self.feishu_chat_id,
                content=message,
                use_webhook=False
            )
            
            if success:
                logger.info("代币分析结果已成功发送到飞书")
            else:
                logger.error("发送代币分析结果到飞书失败")
            
            return success
            
        except Exception as e:
            logger.error(f"发送代币分析结果到飞书时出错: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def process_meme_file(self, start_index=0, batch_size=10, save_interval=60):
        """
        批量处理meme.xlsx文件中的代币地址
        
        参数:
            start_index: 开始处理的索引
            batch_size: 每批处理的数量
            save_interval: 保存结果的时间间隔(秒)
        """
        try:
            logger.info("开始批量处理meme.xlsx中的代币地址")
            
            if not self.meme_path.exists():
                logger.error("meme.xlsx文件不存在")
                return
            
            # 获取当前时间戳
            current_time = time.strftime('%Y%m%d_%H%M%S')
            output_excel_path = self.data_dir / f'token_trading_data_{current_time}.xlsx'
            
            # 读取Excel文件
            logger.info(f"正在读取文件: {self.meme_path}")
            df = pd.read_excel(self.meme_path)
            
            # 检查必要的列
            if '内容' not in df.columns:
                logger.error("meme.xlsx文件缺少'内容'列")
                return
            
            logger.info(f"总行数: {len(df)}")
            logger.info(f"将从第 {start_index} 个代币开始处理...")
            
            # 添加需要的列
            columns = [
                'token_id', 'address', 'symbol', 'name', 'network',
                'fdv_usd', 'fdv_usd_formatted', 'volume_usd_24h', 'volume_usd_24h_formatted',
                'price_change_m5', 'price_change_m5_formatted', 'price_change_h1', 'price_change_h1_formatted',
                'm5_buys', 'm5_sells', 'm15_buys', 'm15_sells', 'pool_created_at'
            ]
            
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            
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
                    
                    # 注册到整合器
                    self.integrator.register_token(token_address)
                    
                    # 分析代币
                    token_data = await self.analyze_token(token_address)
                    
                    if token_data:
                        # 将数据更新到DataFrame
                        for key, value in token_data.items():
                            if key in df.columns:
                                df.at[index, key] = value
                        
                        # 不再直接发送到飞书
                        # await self.send_token_analysis_to_feishu(token_data)
                        
                        modified = True
                        success_count += 1
                        
                    else:
                        error_count += 1
                        logger.error(f"无法获取 {token_address} 的交易数据")
                    
                    # 定期保存结果
                    current_time = time.time()
                    if modified and (processed_count % batch_size == 0 or current_time - last_save_time >= save_interval):
                        try:
                            logger.info(f"准备保存进度，已处理 {processed_count} 条数据...")
                            temp_file = str(output_excel_path).replace('.xlsx', '_temp.xlsx')
                            df.to_excel(temp_file, index=False)
                            if output_excel_path.exists():
                                output_excel_path.unlink()
                            os.rename(temp_file, output_excel_path)
                            logger.info(f"已保存当前进度到: {output_excel_path}")
                            last_save_time = current_time
                            modified = False
                        except Exception as save_error:
                            logger.error(f"保存文件时出错: {str(save_error)}")
                    
                    time.sleep(1)  # 添加延迟以避免触发API限制
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"处理代币时出错: {str(e)}")
                    continue
            
            # 最后保存一次
            if modified:
                try:
                    temp_file = str(output_excel_path).replace('.xlsx', '_temp.xlsx')
                    df.to_excel(temp_file, index=False)
                    if output_excel_path.exists():
                        output_excel_path.unlink()
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
            logger.error(f"批量处理过程中出现错误: {str(e)}")
            logger.error(traceback.format_exc())

class MemeAnalysisMonitor:
    def __init__(self):
        # 从配置文件加载飞书配置
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 获取飞书配置
        self.app_id = config.get('feishu_app_id', 'cli_a736cea2ff78100d')
        self.app_secret = config.get('feishu_app_secret', 'C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS')
        
        # 初始化飞书机器人，传入必要的参数
        self.feishu_bot = FeishuBot(app_id=self.app_id, app_secret=self.app_secret)
        self.feishu_chat_id = config.get('feishu_chat_id', 'oc_a2d2c5616c900bda2ab8e13a77361287')
        self.data_dir = Path('data')
        self.meme_file = self.data_dir / 'meme.xlsx'  # 修改为监控 meme.xlsx
        self.last_modified_time = None
        self.last_processed_index = -1  # 记录最后处理的行索引
        
        # 初始化时读取当前文件的行数
        self._init_last_processed_index()
        
    def _init_last_processed_index(self):
        """初始化时读取当前文件的行数"""
        try:
            if self.meme_file.exists():
                df = pd.read_excel(self.meme_file)
                self.last_processed_index = len(df) - 1  # 设置为最后一行的索引
                logger.info(f"初始化完成，当前 meme.xlsx 文件共有 {self.last_processed_index + 1} 行")
        except Exception as e:
            logger.error(f"初始化最后处理索引时出错: {str(e)}")
            self.last_processed_index = -1

    def monitor_analysis_file(self, interval: int = 5):
        """
        监控 meme.xlsx 文件的更新
        :param interval: 检查间隔（秒）
        """
        logging.info(f"开始监控文件: {self.meme_file}")
        
        while True:
            try:
                if not self.meme_file.exists():
                    logging.warning("meme.xlsx 文件不存在")
                    time.sleep(interval)
                    continue

                current_mtime = os.path.getmtime(self.meme_file)
                
                # 检查文件是否更新
                if self.last_modified_time is None or current_mtime > self.last_modified_time:
                    logging.info("检测到 meme.xlsx 文件更新，处理新数据...")
                    self._process_new_data()
                    self.last_modified_time = current_mtime
                
                time.sleep(interval)
                
            except Exception as e:
                logging.error(f"监控文件时发生错误: {str(e)}")
                time.sleep(interval)

    def _process_new_data(self):
        """处理新的 meme 数据"""
        try:
            df = pd.read_excel(self.meme_file)
            current_rows = len(df)
            
            # 如果有新行
            if current_rows > self.last_processed_index + 1:
                # 只处理新增的行
                new_rows = df.iloc[self.last_processed_index + 1:]
                logging.info(f"发现 {len(new_rows)} 条新数据")
                
                # 处理每一行新数据
                for _, row in new_rows.iterrows():
                    token_address = row['内容']
                    if pd.isna(token_address):
                        continue
                        
                    # 注册到整合器进行处理
                    integrator.register_token(token_address)
                
                # 更新最后处理的索引
                self.last_processed_index = current_rows - 1
                logging.info(f"更新最后处理索引为: {self.last_processed_index}")
                
        except Exception as e:
            logging.error(f"处理新数据时发生错误: {str(e)}")

# 新增文件监控类
class MemeFileWatcher(FileSystemEventHandler):
    """监控 meme.xlsx 文件的变化并处理新增数据"""
    
    def __init__(self, meme_file_path, analyzer=None, coingecko_analyzer=None):
        super().__init__()
        self.meme_file_path = meme_file_path
        self.analyzer = analyzer
        self.coingecko_analyzer = coingecko_analyzer
        self.last_processed_row = 0
        self.last_modified_time = self._get_file_mtime()
        
        # 加载配置
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 初始化飞书机器人
        self.app_id = config.get('feishu_app_id', 'cli_a736cea2ff78100d')
        self.app_secret = config.get('feishu_app_secret', 'C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS')
        self.feishu_bot = FeishuBot(app_id=self.app_id, app_secret=self.app_secret)
        self.feishu_chat_id = config.get('feishu_chat_id', 'oc_a2d2c5616c900bda2ab8e13a77361287')
        
        # 初始化Telegram机器人
        telegram_token = config.get('telegram_token', '')
        if telegram_token:
            self.telegram_bot = TelegramBot(token=telegram_token)
        else:
            self.telegram_bot = None
        self.telegram_chat_id = config.get('telegram_chat_id', '')
        
        # 添加时间窗口和计数逻辑
        self.token_occurrences = {}  # 记录代币出现次数和时间
        self.time_window = 600  # 10分钟 = 600秒
        self.occurrence_threshold = 3  # 出现3次才报警
        
        # 添加报警历史记录
        self.alert_history = {}  # 记录代币的报警时间
        self.alert_cooldown = 3600  # 1小时 = 3600秒
        
        # 添加消息推送状态管理
        self.message_status = {}  # 记录消息推送状态
        self.message_lock = threading.Lock()  # 添加线程锁
        
        # 初始化时检查文件是否存在，记录当前行数
        self._check_initial_state()
        
        logger.info(f"文件监控器已初始化，监控文件: {meme_file_path}")
        logger.info(f"当前记录的行数: {self.last_processed_row}")

    def _check_initial_state(self):
        """检查文件初始状态，记录当前行数"""
        if os.path.exists(self.meme_file_path):
            try:
                df = pd.read_excel(self.meme_file_path)
                self.last_processed_row = len(df)
                logger.info(f"初始文件包含 {self.last_processed_row} 行数据")
            except Exception as e:
                logger.error(f"读取初始文件时出错: {str(e)}")
                self.last_processed_row = 0
        else:
            logger.warning(f"监控的文件 {self.meme_file_path} 不存在")
            self.last_processed_row = 0

    def _get_file_mtime(self):
        """获取文件最后修改时间"""
        if os.path.exists(self.meme_file_path):
            return os.path.getmtime(self.meme_file_path)
        return 0

    def _check_token_occurrence(self, token_address):
        """检查代币在时间窗口内的出现次数"""
        current_time = time.time()
        
        # 如果代币不在记录中，初始化记录
        if token_address not in self.token_occurrences:
            self.token_occurrences[token_address] = {
                'count': 1,
                'first_seen': current_time,
                'last_seen': current_time
            }
            return False
        
        # 获取代币记录
        record = self.token_occurrences[token_address]
        
        # 检查是否在时间窗口内
        if current_time - record['first_seen'] <= self.time_window:
            # 在时间窗口内，增加计数
            record['count'] += 1
            record['last_seen'] = current_time
            
            # 如果达到阈值，返回True
            if record['count'] >= self.occurrence_threshold:
                logger.info(f"代币 {token_address} 在10分钟内出现 {record['count']} 次，触发报警")
                return True
        else:
            # 超出时间窗口，重置计数
            record['count'] = 1
            record['first_seen'] = current_time
            record['last_seen'] = current_time
        
        return False

    def _cleanup_old_records(self):
        """清理过期的记录"""
        current_time = time.time()
        expired_tokens = [
            token for token, record in self.token_occurrences.items()
            if current_time - record['last_seen'] > self.time_window
        ]
        for token in expired_tokens:
            del self.token_occurrences[token]

    def _check_alert_history(self, token_address):
        """检查代币是否在冷却期内"""
        current_time = time.time()
        
        if token_address in self.alert_history:
            last_alert_time = self.alert_history[token_address]
            if current_time - last_alert_time <= self.alert_cooldown:
                logger.info(f"代币 {token_address} 在1小时内已经报警过，跳过")
                return False
            else:
                # 超过冷却期，更新报警时间
                self.alert_history[token_address] = current_time
                return True
        else:
            # 首次报警，记录时间
            self.alert_history[token_address] = current_time
            return True

    def _cleanup_alert_history(self):
        """清理过期的报警记录"""
        current_time = time.time()
        expired_tokens = [
            token for token, alert_time in self.alert_history.items()
            if current_time - alert_time > self.alert_cooldown
        ]
        for token in expired_tokens:
            del self.alert_history[token]

    def _check_message_status(self, token_address):
        """检查消息是否已经推送过"""
        with self.message_lock:
            if token_address in self.message_status:
                status = self.message_status[token_address]
                # 如果消息已经推送成功，返回 False
                if status.get('sent', False):
                    return False
                # 如果消息正在处理中，返回 False
                if status.get('processing', False):
                    return False
            return True

    def _update_message_status(self, token_address, status):
        """更新消息状态"""
        with self.message_lock:
            self.message_status[token_address] = status

    def _cleanup_message_status(self):
        """清理过期的消息状态"""
        current_time = time.time()
        with self.message_lock:
            expired_tokens = [
                token for token, status in self.message_status.items()
                if current_time - status.get('timestamp', 0) > self.alert_cooldown
            ]
            for token in expired_tokens:
                del self.message_status[token]

    def on_modified(self, event):
        """当文件被修改时处理新数据"""
        if not isinstance(event, FileModifiedEvent):
            return
            
        # 检查是否是目标文件
        if event.src_path != str(self.meme_file_path):
            return
            
        # 检查修改时间，避免重复处理
        current_mtime = self._get_file_mtime()
        if current_mtime == self.last_modified_time:
            return
            
        self.last_modified_time = current_mtime
        
        # 等待文件完全写入
        time.sleep(1)
        
        # 处理新数据
        self._process_new_data()

    def _process_new_data(self):
        """处理新增数据行"""
        try:
            # 清理过期记录
            self._cleanup_old_records()
            self._cleanup_alert_history()
            self._cleanup_message_status()  # 清理过期的消息状态
            
            # 读取当前文件
            df = pd.read_excel(self.meme_file_path)
            current_rows = len(df)
            
            # 检查是否有新行
            if current_rows <= self.last_processed_row:
                logger.info("没有检测到新的数据行")
                return
                
            # 处理新增的行
            new_rows = df.iloc[self.last_processed_row:current_rows]
            logger.info(f"检测到 {len(new_rows)} 行新数据")
            
            # 为每一个新行异步处理数据
            for idx, row in new_rows.iterrows():
                token_address = row['内容']
                if pd.isna(token_address) or not token_address:
                    logger.warning(f"跳过空地址，索引 {idx}")
                    continue
                    
                logger.info(f"处理新增代币: {token_address}")
                
                # 检查代币出现次数和报警历史
                if self._check_token_occurrence(token_address) and self._check_alert_history(token_address):
                    # 异步处理新的代币数据
                    asyncio.run(self._analyze_token(token_address))
                else:
                    logger.info(f"代币 {token_address} 未达到报警阈值或在冷却期内，跳过处理")
            
            # 更新处理过的行数
            self.last_processed_row = current_rows
            logger.info(f"已处理到第 {self.last_processed_row} 行")
            
        except Exception as e:
            logger.error(f"处理文件更新时出错: {str(e)}")
            logger.error(traceback.format_exc())

    async def _analyze_token(self, token_address):
        """分析单个代币数据并发送到飞书"""
        try:
            # 检查消息状态
            if not self._check_message_status(token_address):
                logger.info(f"代币 {token_address} 的消息已经处理过或正在处理中，跳过")
                return

            # 更新状态为处理中
            self._update_message_status(token_address, {
                'processing': True,
                'timestamp': time.time()
            })

            twitter_analysis = None
            coingecko_data = None
            
            # 1. Twitter 分析
            if self.analyzer:
                logger.info(f"开始对 {token_address} 进行 Twitter 分析")
                max_retries = 5
                retry_delay = 30
                
                for attempt in range(max_retries):
                    try:
                        tweets = await twitter_api.search_tweets(token_address)
                        if tweets:
                            logger.info(f"找到 {len(tweets)} 条相关推文")
                            twitter_analysis = await self.analyzer.analyze_tweets(token_address, tweets)
                            logger.info(f"已完成 {token_address} 的 Twitter 分析")
                            break
                        else:
                            logger.warning(f"未找到关于 {token_address} 的推文")
                            break
                    except Exception as e:
                        if "Rate limit exceeded" in str(e):
                            if attempt < max_retries - 1:
                                current_delay = retry_delay * (2 ** attempt)
                                logger.warning(f"Twitter API 速率限制，第 {attempt + 1} 次重试，等待 {current_delay} 秒...")
                                await asyncio.sleep(current_delay)
                                continue
                        logger.error(f"Twitter 分析出错: {str(e)}")
                        break
            
            # 2. CoinGecko 分析
            if self.coingecko_analyzer:
                logger.info(f"开始对 {token_address} 进行 CoinGecko 分析")
                coingecko_data = await self.coingecko_analyzer.analyze_token(token_address)
                if coingecko_data:
                    logger.info(f"已完成 {token_address} 的 CoinGecko 分析")
                else:
                    logger.warning(f"CoinGecko 无法分析代币 {token_address}")
            
            # 3. 发送分析结果到飞书 - 这部分需要修改
            if twitter_analysis or coingecko_data:
                # 不再立即发送，而是只保存分析结果
                message = self._build_analysis_message(token_address, twitter_analysis, coingecko_data)
                if message:
                    # 更新消息状态，但不发送
                    self._update_message_status(token_address, {
                        'processed': True,  # 标记为已处理
                        'message': message,  # 保存消息内容
                        'timestamp': time.time()
                    })
                    
                    logger.info(f"已完成 {token_address} 的分析，等待所有分析完成后统一发送")
                    
        except Exception as e:
            logger.error(f"分析代币 {token_address} 时出错: {str(e)}")
            logger.error(traceback.format_exc())

    def _build_analysis_message(self, token_address, twitter_analysis, coingecko_data):
        """构建分析结果消息"""
        try:
            message = f"""🔍金狗预警

📌 代币地址: {token_address}"""

            if coingecko_data:
                message += f"""
🪙 名称: {coingecko_data.get('name', 'N/A')} ({coingecko_data.get('symbol', 'N/A')})
🌐 网络: {coingecko_data.get('network', 'N/A')}

💰 市场数据:
• 市值: {coingecko_api.format_currency(coingecko_data.get('fdv_usd', 'N/A'))}
• 24小时交易量: {coingecko_api.format_currency(coingecko_data.get('volume_usd_24h', 'N/A'))}
• 创建时间: {coingecko_data.get('pool_created_at', 'N/A')}

📈 价格变动:
• 5分钟: {coingecko_api.format_percentage(coingecko_data.get('price_change_m5', 'N/A'))}
• 1小时: {coingecko_api.format_percentage(coingecko_data.get('price_change_h1', 'N/A'))}

🔄 最近交易次数:
• 5分钟内: 买入 {coingecko_data.get('m5_buys', 0)} 次, 卖出 {coingecko_data.get('m5_sells', 0)} 次
• 15分钟内: 买入 {coingecko_data.get('m15_buys', 0)} 次, 卖出 {coingecko_data.get('m15_sells', 0)} 次"""

            if twitter_analysis:
                message += f"""

📝 叙事信息:
{twitter_analysis.get('叙事信息', 'N/A')}

🌡️ 可持续性分析:
• 社区热度: {twitter_analysis.get('可持续性_社区热度', 'N/A')}
• 传播潜力: {twitter_analysis.get('可持续性_传播潜力', 'N/A')}
• 短期投机价值: {twitter_analysis.get('可持续性_短期投机价值', 'N/A')}"""

            return message
            
        except Exception as e:
            logger.error(f"构建分析消息时出错: {str(e)}")
            return None

    def start_watching(self):
        """开始监控文件"""
        observer = Observer()
        # 监控文件所在目录
        directory = os.path.dirname(self.meme_file_path)
        observer.schedule(self, directory, recursive=False)
        observer.start()
        logger.info(f"开始监控目录: {directory}")
        return observer

# 修改主函数
async def main():
    try:
        # 解析命令行参数
        parser = argparse.ArgumentParser(description='Meme 币分析工具')
        parser.add_argument('--coingecko', action='store_true', help='只运行 CoinGecko 分析')
        parser.add_argument('--twitter', action='store_true', help='只运行 Twitter 分析')
        parser.add_argument('--start', type=int, default=0, help='CoinGecko 分析的起始索引')
        parser.add_argument('--batch', type=int, default=10, help='CoinGecko 批处理大小')
        parser.add_argument('--watch', action='store_true', help='启用文件监控模式')
        args = parser.parse_args()
        
        # 根据命令行参数决定运行哪些分析
        run_twitter = not args.coingecko or args.twitter
        run_coingecko = not args.twitter or args.coingecko
        watch_mode = args.watch
        
        # 配置整合器是否需要两种分析都完成
        if run_twitter and run_coingecko:
            # 需要两种分析都完成
            pass
        else:
            # 修改整合器方法，允许只有一种分析完成就发送
            integrator._need_both_analyses = lambda: False
        
        # 初始化分析器实例
        twitter_analyzer = None
        coingecko_analyzer = None
        
        if run_twitter:
            logger.info("初始化 Twitter Meme 分析器...")
            twitter_analyzer = MemeAnalyzer()
            
        if run_coingecko:
            logger.info("初始化 CoinGecko 代币分析器...")
            coingecko_analyzer = CoinGeckoAnalyzer()
            
        # 启用文件监控模式
        if watch_mode:
            logger.info("启动文件监控模式...")
            meme_file_path = Path('data') / 'meme.xlsx'
            
            # 创建文件监控器
            watcher = MemeFileWatcher(
                meme_file_path=meme_file_path,
                analyzer=twitter_analyzer,
                coingecko_analyzer=coingecko_analyzer
            )
            
            # 开始监控
            observer = watcher.start_watching()
            
            # 保持主线程运行
            try:
                logger.info("文件监控已启动，按 Ctrl+C 停止...")
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("收到停止信号，正在停止监控...")
                observer.stop()
            observer.join()
        else:
            # 使用传统模式，处理整个文件
            if run_twitter and twitter_analyzer:
                logger.info("启动 Twitter Meme 分析...")
                await twitter_analyzer.process_history_file()
            
            if run_coingecko and coingecko_analyzer:
                logger.info("启动 CoinGecko 代币分析...")
                await coingecko_analyzer.process_meme_file(
                    start_index=args.start, 
                    batch_size=args.batch
                )
            
            # 保持主线程运行，等待用户中断
            logger.info("程序正在运行中，按 Ctrl+C 退出...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("接收到中断信号，程序退出...")
        
    except Exception as e:
        logger.error(f"运行时发生错误: {str(e)}")
        logger.exception(e)

# 修改 __main__ 部分
if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 检查是否有命令行参数
    if len(sys.argv) > 1:
        # 有命令行参数，使用异步运行
        asyncio.run(main())
    else:
        # 没有命令行参数，默认启动文件监控模式
        try:
            logger.info("使用默认模式启动程序 - 启动文件监控模式")
            # 设置命令行参数为启动监控模式
            sys.argv.append('--watch')
            asyncio.run(main())
        except Exception as e:
            logger.error(f"运行时发生错误: {str(e)}")
            logger.exception(e)