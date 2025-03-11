#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import json
import sys
import logging
import aiohttp
from datetime import datetime
import os
import socket
from aiohttp_socks import ProxyConnector
from pathlib import Path
import pandas as pd
import time
from typing import Optional, Dict
import re
from discord.ext import commands
import discord

# 设置日志 - 移到最前面
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 在导入discord之前，创建并注入所有需要的假模块
class DummyAudioop:
    def __init__(self):
        pass
    
    def ratecv(self, *args, **kwargs):
        return b'', 0
    
    def tostereo(self, *args, **kwargs):
        return b''

# 创建假的模块类
class DummyModule:
    pass

# 创建假的voice相关类
class DummyVoiceClient:
    warn_nacl = False
    def __init__(self, *args, **kwargs):
        pass

class DummyVoiceProtocol:
    pass

class DummyOpusError(Exception):
    pass

# 创建假的opus模块
dummy_opus = DummyModule()
dummy_opus.is_loaded = lambda: False
dummy_opus.OpusError = DummyOpusError
dummy_opus.OpusNotLoaded = DummyOpusError

# 创建假的nacl模块
dummy_nacl = DummyModule()

# 注入所有假模块
sys.modules['audioop'] = DummyAudioop()
sys.modules['nacl'] = dummy_nacl
sys.modules['discord.voice_client'] = type('voice_client', (), {
    'VoiceClient': DummyVoiceClient,
    'VoiceProtocol': DummyVoiceProtocol
})
sys.modules['discord.opus'] = dummy_opus
sys.modules['discord.player'] = DummyModule()

# 配置管理类
class Config:
    def __init__(self, config_file='config.json', prompts_file='prompts.json'):
        self.config_file = config_file
        self.prompts_file = prompts_file
        self._config = self.load_config(config_file)  # 使用_config存储配置
        self.prompts = self.load_prompts(prompts_file)
        self._use_proxy = True  # 添加代理开关

    def load_config(self, config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载配置文件失败: {str(e)}")
            raise

    def load_prompts(self, prompts_file):
        try:
            with open(prompts_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载提示词文件失败: {str(e)}")
            raise

    # 添加获取配置的方法
    def get_save_path(self):
        return self._config['monitor']['save_path']

    def get_channels(self):
        return self._config['monitor']['channels']

    def get_token(self):
        return self._config['token']

    def get_proxy(self):
        return None  # 临时返回 None 来禁用代理

    def disable_proxy(self):
        self._use_proxy = False
        logger.info("已禁用代理")

    def enable_proxy(self):
        self._use_proxy = True
        logger.info("已启用代理")

    def get_channel_name(self, channel_id):
        """获取频道名称"""
        return self._config['monitor']['channel_names'].get(channel_id, channel_id)

    def get_channel_type(self, channel_id):
        """获取频道类型"""
        return self._config['monitor']['channel_types'].get(channel_id, 'general')

    def get_api_key(self):
        """获取 API key"""
        return self._config.get('api_key')

# API 客户端类
class SiliconFlowClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.com/v1"
        self.session = None
        
    async def _ensure_session(self):
        if self.session is None:
            # 创建带有代理和SSL配置的会话
            connector = ProxyConnector.from_url(
                'http://127.0.0.1:9098',
                ssl=False  # 禁用SSL验证
            )
            
            # 设置超时
            timeout = aiohttp.ClientTimeout(total=300)
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                trust_env=True,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
    
    async def chat_completion(self, messages, model="deepseek-ai/DeepSeek-V3", **kwargs):
        await self._ensure_session()
        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json={
                    "model": model,
                    "messages": messages,
                    **kwargs
                }
            ) as response:
                return await response.json()
        except Exception as e:
            logger.error(f"API调用出错: {str(e)}")
            return None
            
    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

# 消息处理类
class MessageProcessor:
    def __init__(self, config):
        self.config = config
        self.silicon_client = None
        self.message_patterns = {
            'twitter': r'https?://(?:www\.)?twitter\.com/\w+/status/(\d+)',
            'trading_signal': r'(买入|卖出|做多|做空).*?([\d.]+)',
            # 添加更多模式匹配
        }

    async def process_message(self, message):
        """处理消息的主要方法"""
        try:
            channel_id = str(message.channel.id)
            channel_type = self.config.get_channel_type(channel_id)
            
            # 根据频道类型选择处理方法
            if channel_type == "trading":
                return await self.process_trading_message(message)
            elif channel_type == "news":
                return await self.process_news_message(message)
            elif channel_type == "social":
                return await self.process_social_message(message)
            else:
                return await self.process_general_message(message)
                
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            return None

    async def process_trading_message(self, message):
        """处理交易信号消息"""
        try:
            # 使用 AI 分析交易信号
            analysis = await self.analyze_trading_signal(message.content)
            
            # 提取关键信息
            trading_info = {
                'timestamp': message.created_at.isoformat(),
                'channel_id': str(message.channel.id),
                'message_id': str(message.id),
                'content': message.content,
                'analysis': analysis,
                'type': 'trading_signal'
            }
            
            # 保存到数据库或文件
            await self.save_trading_info(trading_info)
            
            # 可以添加通知逻辑
            await self.send_notification(trading_info)
            
            return trading_info
            
        except Exception as e:
            logger.error(f"处理交易消息时发生错误: {str(e)}")
            return None

    async def process_social_message(self, message):
        """处理社交媒体消息"""
        try:
            # 检查是否包含 Twitter 链接
            twitter_urls = re.findall(self.message_patterns['twitter'], message.content)
            
            if twitter_urls:
                # 处理 Twitter 链接
                for tweet_id in twitter_urls:
                    tweet_info = await self.analyze_tweet(tweet_id)
                    if tweet_info:
                        await self.save_tweet_info(tweet_info)
            
            return {
                'type': 'social',
                'platform': 'twitter' if twitter_urls else 'unknown',
                'urls': twitter_urls,
                'content': message.content
            }
            
        except Exception as e:
            logger.error(f"处理社交媒体消息时发生错误: {str(e)}")
            return None

    async def analyze_trading_signal(self, content):
        """使用 AI 分析交易信号"""
        if not self.silicon_client:
            self.silicon_client = SiliconFlowClient(self.config.get_api_key())
        
        prompt = self.config.prompts.get('trading_analysis', '')
        messages = [
            {
                "role": "user",
                "content": f"{prompt}\n\n需要分析的内容:\n{content}"
            }
        ]
        
        response = await self.silicon_client.chat_completion(
            messages=messages,
            model="deepseek-ai/DeepSeek-V3",
            max_tokens=1024,
            temperature=0.7
        )
        
        if response and 'choices' in response:
            return response['choices'][0]['message']['content']
        return None

    async def save_trading_info(self, trading_info):
        """保存交易信息"""
        try:
            # 获取保存路径
            base_dir = os.path.join(os.path.dirname(__file__), 'data', 'trading')
            os.makedirs(base_dir, exist_ok=True)
            
            # 按日期保存
            date_str = datetime.now().strftime('%Y-%m-%d')
            file_path = os.path.join(base_dir, f'trading_{date_str}.json')
            
            # 读取现有数据
            existing_data = []
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # 添加新数据
            existing_data.append(trading_info)
            
            # 保存数据
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"已保存交易信息到: {file_path}")
            
        except Exception as e:
            logger.error(f"保存交易信息时发生错误: {str(e)}")

    async def send_notification(self, info):
        """发送通知"""
        # 这里可以实现通知逻辑，比如：
        # - 发送到 Telegram
        # - 发送到微信
        # - 发送邮件
        # - 发送到其他 Discord 频道
        pass

    async def process_general_message(self, message):
        """处理一般消息"""
        try:
            # 构建消息数据
            message_data = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'channel_id': str(message.channel.id),
                'channel_name': self.config.get_channel_name(str(message.channel.id)),
                'author': str(message.author),
                'author_id': str(message.author.id),
                'content': message.content,
                'attachments': [att.url for att in message.attachments],
                'embeds': [embed.to_dict() for embed in message.embeds],
                'type': 'general'
            }
            
            logger.info(f"处理一般消息: {message_data['content'][:100]}...")
            return message_data
            
        except Exception as e:
            logger.error(f"处理一般消息时出错: {str(e)}")
            return None

# 设置事件循环
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 修补 discord.py 的问题
def patch_discord():
    """修补 discord.py 的问题"""
    def parse_ready_supplemental(self, data):
        """处理 ready_supplemental 事件"""
        try:
            self.pending_payments = {}
            logger.debug("已处理 ready_supplemental 事件")
            return True
        except Exception as e:
            logger.error(f"处理 ready_supplemental 时发生错误: {str(e)}")
            return False

    # 替换原始方法
    discord.state.ConnectionState.parse_ready_supplemental = parse_ready_supplemental

# 应用补丁
patch_discord()

class SimpleDiscordMonitor(discord.Client):  # 改为继承 discord.Client
    async def setup_http_session(self):
        """设置HTTP会话"""
        try:
            # 修改这里的代理地址
            proxy = "http://127.0.0.1:7890"  # 直接使用7890端口
            try:
                connector = ProxyConnector.from_url(
                    proxy,
                    ssl=False,
                    force_close=True,
                    enable_cleanup_closed=True,
                    ttl_dns_cache=300,
                    limit=10,
                    family=socket.AF_INET,
                    rdns=True
                )
                logger.info(f"HTTP会话使用代理: {proxy}")
            except Exception as e:
                logger.error(f"代理设置失败，切换到直连模式: {str(e)}")
                self.config.disable_proxy()
                connector = aiohttp.TCPConnector(
                    ssl=False,
                    force_close=True,
                    enable_cleanup_closed=True,
                    ttl_dns_cache=300,
                    limit=10,
                    family=socket.AF_INET,
                    rdns=True
                )
                logger.info("已切换到直连模式")
            
            timeout = aiohttp.ClientTimeout(
                total=300,
                connect=60,
                sock_connect=60,
                sock_read=60
            )
            
            return aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                trust_env=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
        except Exception as e:
            logger.error(f"设置HTTP会话时发生错误: {str(e)}")
            raise

    def __init__(self, config):
        self.config = config
        
        # 修改这里的代理配置
        proxy = "http://127.0.0.1:7890"
        logger.info(f"使用代理配置: {proxy}")
        
        # 使用 discord.py-self 的正确初始化方式
        super().__init__(
            self_bot=True,  # 必须设置为 True
            chunk_guilds_at_startup=False,
            proxy=proxy,
            max_messages=10000
        )
        
        self.message_processor = MessageProcessor(config)
        self.messages = {}
        self.last_save_time = {}
        
        logger.info("配置文件加载成功")
        
        # 设置保存基础目录
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.save_dir = os.path.join(base_dir, 'data', 'messages')
        
        # 创建消息保存目录
        try:
            os.makedirs(self.save_dir, exist_ok=True)
            logger.info(f"已创建/确认消息保存目录: {self.save_dir}")
        except Exception as e:
            logger.error(f"创建消息保存目录失败: {str(e)}")
            raise
        
        # 初始化每个频道的消息文件
        for channel_id in self.config.get_channels():
            # 获取频道名称
            channel_name = self.config.get_channel_name(channel_id)
            # 使用频道名称作为文件名
            filename = f"{channel_id}-{channel_name}.json"
            # 替换文件名中的非法字符
            filename = "".join(c for c in filename if c.isalnum() or c in ('-', '_', '.'))
            
            channel_file = os.path.join(self.save_dir, filename)
            if os.path.exists(channel_file):
                try:
                    with open(channel_file, 'r', encoding='utf-8') as f:
                        self.messages[channel_id] = json.load(f)
                    logger.info(f"已加载频道 {channel_name} ({channel_id}) 的消息文件")
                except Exception as e:
                    logger.error(f"加载频道 {channel_name} ({channel_id}) 的消息文件失败: {str(e)}")
                    self.messages[channel_id] = []
            else:
                self.messages[channel_id] = []
                try:
                    with open(channel_file, 'w', encoding='utf-8') as f:
                        json.dump([], f, ensure_ascii=False, indent=2)
                    logger.info(f"已创建频道 {channel_name} ({channel_id}) 的消息文件")
                except Exception as e:
                    logger.error(f"创建频道 {channel_name} ({channel_id}) 的消息文件失败: {str(e)}")
                    raise
        
        logger.info("Discord客户端初始化完成")

    def is_monitored_channel(self, message):
        """检查消息是否来自被监控的频道"""
        channel_id = str(message.channel.id)
        
        # 检查是否在监控列表中
        monitored_channels = self.config.get_channels()
        if channel_id in monitored_channels:
            logger.info(f"匹配到监控频道ID: {channel_id}")
            return True
        
        logger.info(f"该频道不在监控列表中: {channel_id}")
        return False

    def save_messages(self, channel_id):
        """保存指定频道的消息到文件"""
        try:
            # 获取频道名称
            channel_name = self.config.get_channel_name(channel_id)
            # 使用频道名称作为文件名
            filename = f"{channel_id}-{channel_name}.json"
            # 替换文件名中的非法字符
            filename = "".join(c for c in filename if c.isalnum() or c in ('-', '_', '.'))
            
            channel_file = os.path.join(self.save_dir, filename)
            with open(channel_file, 'w', encoding='utf-8') as f:
                json.dump(self.messages[channel_id], f, ensure_ascii=False, indent=2)
            logger.info(f"消息已保存到频道 {channel_name} ({channel_id})")
        except Exception as e:
            logger.error(f"保存频道 {channel_id} 的消息时出错: {str(e)}")
            logger.exception(e)

    async def setup_hook(self) -> None:
        """设置钩子，在客户端准备好之前调用"""
        try:
            logger.info("setup_hook 被调用")
            session = await self.setup_http_session()
            self.http.session = session
            logger.info("setup_hook 完成")
            
            # 在这里也添加一个测试日志
            logger.info("等待 on_ready 事件...")
        except Exception as e:
            logger.error(f"设置钩子时发生错误: {str(e)}")
            raise

    async def on_connect(self):
        """当客户端连接到Discord时触发"""
        logger.info("已连接到Discord服务器")

    async def on_disconnect(self):
        """当客户端断开连接时触发"""
        logger.warning("与Discord服务器的连接已断开，尝试重新连接...")

    async def on_error(self, event, *args, **kwargs):
        """当发生错误时触发"""
        logger.error(f"发生错误 - 事件: {event}")
        import traceback
        logger.error(traceback.format_exc())

    async def on_ready(self):
        """当机器人成功登录后触发"""
        try:
            logger.info("\n" + "=" * 50)
            logger.info("Discord Monitor 已就绪")
            logger.info(f"登录账号: {self.user.name}")
            logger.info(f"账号 ID: {self.user.id}")
            
            # 打印所有服务器和频道信息
            guilds = list(self.guilds)
            logger.info(f"\n已加入 {len(guilds)} 个服务器:")
            for guild in guilds:
                logger.info(f"\n服务器: {guild.name} (ID: {guild.id})")
                logger.info("频道列表:")
                for channel in guild.channels:
                    if isinstance(channel, discord.TextChannel):  # 只显示文字频道
                        channel_id = str(channel.id)
                        is_monitored = "✓" if channel_id in self.config.get_channels() else " "
                        logger.info(f"[{is_monitored}] {channel.name} (ID: {channel_id})")
            
            # 打印监控列表
            logger.info("\n监控的频道ID:")
            for channel_id in self.config.get_channels():
                logger.info(f"- {channel_id}")
            
            logger.info("=" * 50 + "\n")
            logger.info("开始监控消息...")
            
        except Exception as e:
            logger.error(f"on_ready 事件处理出错: {str(e)}")
            logger.exception(e)

    async def analyze_channel_content(self, channel_id: str, content: str) -> Optional[Dict]:
        """使用 SiliconFlow API 分析频道内容"""
        try:
            prompt = self.config.prompts.get(channel_id)
            if not prompt:
                return None
                
            messages = [
                {
                    "role": "user",
                    "content": f"{prompt}\n\n需要分析的内容:\n{content}"
                }
            ]
            
            response = self.silicon_client.chat_completion(
                messages=messages,
                model="deepseek-ai/DeepSeek-V3",
                max_tokens=512,
                temperature=0.7
            )
            
            return response
            
        except Exception as e:
            logger.error(f"分析频道内容时出错: {str(e)}")
            return None

    async def handle_trading_channel(self, message):
        """处理交易频道的消息"""
        try:
            channel_id = str(message.channel.id)
            # 从prompts.json中获取提示词
            analysis_prompt = self.config.prompts.get(channel_id)
            
            if not analysis_prompt:
                logger.error(f"频道 {channel_id} 未找到提示词配置")
                return False
            
            # 构建消息列表
            messages = [
                {
                    "role": "user",
                    "content": analysis_prompt.format(content=message.content)
                }
            ]
            
            # 调用API
            logger.info("开始调用API进行分析...")
            response = await self.silicon_client.chat_completion(
                messages=messages,
                model="deepseek-ai/DeepSeek-V3",
                max_tokens=1024,
                temperature=0.7
            )
            
            if response and 'choices' in response:
                analysis_result = response['choices'][0]['message']['content']
                logger.info("API返回原始结果：")
                logger.info(analysis_result)
                
                # 清理API返回的结果
                cleaned_result = analysis_result
                if cleaned_result.startswith('```json'):
                    cleaned_result = cleaned_result.replace('```json', '', 1)
                if cleaned_result.endswith('```'):
                    cleaned_result = cleaned_result.replace('```', '', 1)
                cleaned_result = cleaned_result.strip()
                
                # 解析JSON结果
                analysis_json = json.loads(cleaned_result)
                
                # 获取当前时间
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 创建空的DataFrame列表
                df_list = []
                
                # 处理每个币种的分析结果
                for coin_data in analysis_json:
                    analysis_data = {
                        '时间': current_time,
                        '频道ID': str(message.channel.id),
                        '原始内容': message.content,
                        '交易币种': coin_data.get('交易币种'),
                        '方向': coin_data.get('交易方向'),
                        '杠杆倍数': coin_data.get('杠杆倍数'),
                        '入场点位1': coin_data.get('入场点位', [None, None, None])[0],
                        '入场点位2': coin_data.get('入场点位', [None, None, None])[1],
                        '入场点位3': coin_data.get('入场点位', [None, None, None])[2],
                        '止损点位1': coin_data.get('止损点位', [None, None, None])[0],
                        '止损点位2': coin_data.get('止损点位', [None, None, None])[1],
                        '止损点位3': coin_data.get('止损点位', [None, None, None])[2],
                        '止盈点位1': coin_data.get('止盈点位', [None, None, None])[0],
                        '止盈点位2': coin_data.get('止盈点位', [None, None, None])[1],
                        '止盈点位3': coin_data.get('止盈点位', [None, None, None])[2],
                        '分析内容': coin_data.get('市场分析')
                    }
                    df_list.append(pd.DataFrame([analysis_data]))
                    
                    # 打印分析结果
                    logger.info(f"\n{coin_data.get('交易币种')}的分析结果：")
                    for key, value in analysis_data.items():
                        logger.info(f"{key}: {value}")
                
                # 合并所有结果
                if df_list:
                    result_df = pd.concat(df_list, ignore_index=True)
                    
                    # 修改保存路径
                    base_dir = os.path.dirname(os.path.abspath(__file__))
                    analysis_path = os.path.join(base_dir, 'data', 'trading_analysis.xlsx')
                    os.makedirs(os.path.dirname(analysis_path), exist_ok=True)
                    
                    try:
                        if os.path.exists(analysis_path):
                            existing_df = pd.read_excel(analysis_path)
                            result_df = pd.concat([existing_df, result_df], ignore_index=True)
                        
                        result_df.to_excel(analysis_path, index=False)
                        logger.info(f"\n分析结果已保存到: {analysis_path}")
                        return True
                        
                    except Exception as e:
                        logger.error(f"保存Excel文件时出错: {e}")
                        return False
                else:
                    logger.error("没有找到有效的分析结果")
                    return False
                    
            else:
                logger.error("API返回结果为空")
                return False
                
        except Exception as e:
            logger.error(f"处理交易频道消息时发生错误: {str(e)}")
            return False

    async def handle_meme_channel(self, message):
        """处理meme频道的消息"""
        logger.info("正在处理meme频道的消息")
        # 这里添加处理meme频道消息的逻辑

    async def on_message(self, message):
        """处理收到的消息"""
        try:
            if message.author == self.user:
                return

            processed = await self.message_processor.process_message(message)
            if processed:
                logger.info(f"处理结果: {processed.get('content', '')[:100]}...")
                
            # 检查是否是监控的频道
            if not self.is_monitored_channel(message):
                return
                
            # 获取频道ID
            channel_id = str(message.channel.id)
            channel_type = self.config.get_channel_type(channel_id)
            
            # 根据频道类型选择处理方法
            if channel_type == "trading":
                await self.handle_trading_channel(message)
            elif channel_type == "meme":
                await self.handle_meme_channel(message)
            else:
                await self.handle_general_channel(message)
                
            # 保存消息
            await self.save_message(message)
            
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            logger.exception(e)

    # 不同频道的处理方法
    async def handle_general_channel(self, message):
        """处理general频道的消息"""
        logger.info("正在处理general频道的消息")
        # 这里添加特定的处理逻辑
        
    async def handle_other_channels(self, message):
        """处理其他频道的消息"""
        logger.info("正在处理其他频道的消息")
        # 这里添加默认的处理逻辑

    async def save_message(self, message):
        """保存单条消息"""
        try:
            channel_id = str(message.channel.id)
            
            # 构建消息数据
            message_data = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'author': str(message.author),
                'author_id': str(message.author.id),
                'content': message.content,
                'attachments': [att.url for att in message.attachments],
                'embeds': [embed.to_dict() for embed in message.embeds]
            }
            
            # 确保频道的消息列表已初始化
            if channel_id not in self.messages:
                self.messages[channel_id] = []
            
            # 添加到对应频道的消息列表
            self.messages[channel_id].append(message_data)
            
            # 保存到文件
            self.save_messages(channel_id)
            
            logger.info(f"消息已保存到频道 {channel_id}")
            
        except Exception as e:
            logger.error(f"保存消息时出错: {str(e)}")
            logger.exception(e)

    async def check_proxy(self):
        """检查代理是否可用"""
        try:
            proxy = self.config.get_proxy()
            async with aiohttp.ClientSession() as session:
                async with session.get('http://httpbin.org/ip', proxy=proxy) as response:
                    if response.status == 200:
                        logger.info(f"代理可用: {proxy}")
                        return True
                    else:
                        logger.error(f"代理不可用: {proxy}")
                        return False
        except Exception as e:
            logger.error(f"检查代理时发生错误: {str(e)}")
            return False

def main():
    try:
        logger.info("正在启动Discord监控...")
        config = Config()  # 加载配置
        client = SimpleDiscordMonitor(config)
        
        # 添加信号处理
        import signal
        def signal_handler(sig, frame):
            logger.info("正在关闭客户端...")
            asyncio.create_task(client.close())
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("开始运行客户端...")
        client.run(config.get_token())  # 使用方法获取token
    except discord.LoginFailure:
        logger.error("登录失败！请检查token是否正确")
    except Exception as e:
        logger.error(f"运行时发生错误: {str(e)}")
        logger.exception(e)

if __name__ == '__main__':
    main()