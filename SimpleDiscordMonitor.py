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

# 设置事件循环
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 设置日志
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

# 现在可以安全地导入 discord
import discord
from discord.ext import commands

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

class SimpleDiscordMonitor(discord.Client):
    def load_config(self):
        """加载配置文件"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), 'config.json')
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("配置文件加载成功")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def __init__(self):
        try:
            # 先加载配置文件
            self.config = {}  # 初始化配置字典
            self.load_config()  # 加载配置
            
            # 设置 intents
            intents = discord.Intents.all()  # 使用 all() 而不是 default()
            
            # 从配置文件获取代理设置
            proxy = self.config.get('proxy', "http://127.0.0.1:7890")
            
            # discord.py-self 的初始化参数
            super().__init__(
                max_messages=10000,
                proxy=proxy,
                chunk_guilds_at_startup=False,
                heartbeat_timeout=300.0,
                guild_ready_timeout=60.0,
                assume_unsync_clock=True
            )  # discord.py-self 不需要 intents 参数
            
            # 创建保存目录
            self.save_dir = os.path.join(os.path.dirname(__file__), 'data')
            os.makedirs(self.save_dir, exist_ok=True)
            logger.info(f"已创建/确认保存目录: {self.save_dir}")
            
            # 初始化消息列表
            self.messages = []
            self.save_path = os.path.join(self.save_dir, 'messages.json')
            
        except Exception as e:
            logger.error(f"初始化时发生错误: {str(e)}")
            raise

    def load_messages(self):
        """加载已保存的消息"""
        try:
            if os.path.exists(self.save_path):
                with open(self.save_path, 'r', encoding='utf-8') as f:
                    self.messages = json.load(f)
                logger.info(f"已加载消息文件: {self.save_path}")
            else:
                self.messages = []
                logger.info("未找到消息文件，创建新的消息列表")
        except Exception as e:
            logger.error(f"加载消息文件失败: {str(e)}")
            self.messages = []

    async def setup_http_session(self):
        """设置HTTP会话"""
        try:
            # 修改连接器配置，将端口改为 9098
            connector = ProxyConnector.from_url(
                'http://127.0.0.1:9098',  # 修改这里的端口
                ssl=False,
                force_close=True,
                enable_cleanup_closed=True,
                ttl_dns_cache=300,
                limit=10,
                family=socket.AF_INET,
                rdns=True
            )
            
            # 设置更长的超时时间
            timeout = aiohttp.ClientTimeout(
                total=300,        # 总超时时间增加到5分钟
                connect=60,       # 连接超时增加到1分钟
                sock_connect=60,  # socket连接超时增加到1分钟
                sock_read=60      # socket读取超时增加到1分钟
            )
            
            logger.info(f"HTTP会话使用代理: http://127.0.0.1:9098")  # 更新日志信息
            
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

    def is_monitored_channel(self, message):
        """检查消息是否来自被监控的频道"""
        if not message.guild:
            logger.info("收到私信消息，已忽略")
            return False
            
        channel_id = str(message.channel.id)
        server_name = message.guild.name
        channel_name = message.channel.name
        
        logger.info(f"收到消息:")
        logger.info(f"- 服务器: {server_name}")
        logger.info(f"- 频道: {channel_name}")
        logger.info(f"- 频道ID: {channel_id}")
        
        # 直接检查频道ID是否在监控列表中
        monitored_channels = self.config['monitor']['channels']
        if channel_id in monitored_channels:
            logger.info(f"匹配到监控频道ID: {channel_id}")
            return True
        
        logger.info(f"该频道不在监控列表中")
        logger.info(f"监控列表: {monitored_channels}")
        return False

    def save_messages(self):
        """保存消息到文件"""
        try:
            # 获取完整的文件路径
            full_path = os.path.abspath(self.save_path)
            
            # 确保目录存在
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(self.messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"消息已保存到: {full_path}")
        except Exception as e:
            logger.error(f"保存消息时出错: {str(e)}")
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
                        is_monitored = "✓" if channel_id in self.config['monitor']['channels'] else " "
                        logger.info(f"[{is_monitored}] {channel.name} (ID: {channel_id})")
            
            # 打印监控列表
            logger.info("\n监控的频道ID:")
            for channel_id in self.config['monitor']['channels']:
                logger.info(f"- {channel_id}")
            
            logger.info("=" * 50 + "\n")
            logger.info("开始监控消息...")
            
        except Exception as e:
            logger.error(f"on_ready 事件处理出错: {str(e)}")
            logger.exception(e)

    async def on_message(self, message):
        """当收到消息时触发"""
        try:
            # 忽略自己的消息
            if message.author == self.user:
                logger.info("(这是自己的消息，已忽略)")
                return

            # 记录消息信息
            print("\n" + "=" * 50)
            print(f"收到新消息:")
            print(f"服务器: {message.guild.name if message.guild else '私信'}")
            print(f"频道: {'私信' if isinstance(message.channel, (discord.DMChannel, discord.PartialMessageable)) else message.channel.name}")
            print(f"频道ID: {message.channel.id}")
            print(f"发送者: {message.author.name} ({message.author.id})")
            print(f"时间: {message.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 处理普通文本内容
            if message.content:
                print(f"文本内容: {message.content}")
            
            # 处理嵌入内容
            if message.embeds:
                print("嵌入内容:")
                for embed in message.embeds:
                    if embed.title:
                        print(f"- 标题: {embed.title}")
                    if embed.description:
                        print(f"- 描述: {embed.description}")
                    if embed.fields:
                        print("- 字段:")
                        for field in embed.fields:
                            print(f"  • {field.name}: {field.value}")
            
            # 处理附件
            if message.attachments:
                print("附件:")
                for att in message.attachments:
                    print(f"- {att.url}")
            print("=" * 50 + "\n")

            # 先保存消息，包含嵌入内容
            message_data = {
                "timestamp": message.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "guild_id": str(message.guild.id) if message.guild else None,
                "guild_name": message.guild.name if message.guild else None,
                "channel_id": str(message.channel.id),
                "channel_name": '私信' if isinstance(message.channel, (discord.DMChannel, discord.PartialMessageable)) else message.channel.name,
                "author_id": str(message.author.id),
                "author_name": message.author.name,
                "content": message.content,
                "embeds": [{
                    "title": embed.title,
                    "description": embed.description,
                    "fields": [{
                        "name": field.name,
                        "value": field.value
                    } for field in embed.fields]
                } for embed in message.embeds] if message.embeds else [],
                "attachments": [att.url for att in message.attachments]
            }
            
            self.messages.append(message_data)
            self.save_messages()
            logger.info("消息已保存")

            # 然后检查是否是目标频道的消息
            if str(message.channel.id) in ["1283359910788202499", "1242865180371587082"]:
                logger.info(f"检测到目标频道消息: {message.channel.id}")
                
                try:
                    # 准备Excel文件路径
                    excel_path = Path('data/discord_messages.xlsx')
                    twitter_results_path = Path('data/twitter_results.xlsx')
                    meme_path = Path('data/meme.xlsx')
                    
                    # 确保目录存在
                    for path in [excel_path, twitter_results_path, meme_path]:
                        path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 只保存时间和嵌入内容的描述
                    message_row = {
                        '时间': message.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        '描述': ''
                    }
                    
                    # 处理嵌入内容和消息内容
                    descriptions = []
                    search_terms = []
                    meme_data = []
                    
                    # 处理嵌入内容中的描述
                    for embed in message.embeds:
                        if embed.description:
                            descriptions.append(embed.description)
                            if embed.description.strip().startswith('['):
                                import re
                                matches = re.findall(r'\[(.*?)\]', embed.description)
                                if matches:
                                    search_terms.extend(matches)
                                    for match in matches:
                                        meme_row = {
                                            '时间': message.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                            '内容': match,
                                            '频道ID': str(message.channel.id)
                                        }
                                        meme_data.append(meme_row)
                    
                    # 处理普通消息内容中的```内容
                    if str(message.channel.id) == "1242865180371587082" and message.content:
                        matches = re.findall(r'```(.*?)```', message.content, re.DOTALL)
                        if matches:
                            for match in matches:
                                meme_row = {
                                    '时间': message.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                    '内容': match.strip(),
                                    '频道ID': str(message.channel.id)
                                }
                                meme_data.append(meme_row)
                    
                    message_row['描述'] = '\n'.join(descriptions)
                    
                    # 保存meme数据到Excel
                    if meme_data:
                        try:
                            if meme_path.exists():
                                try:
                                    df_meme = pd.read_excel(meme_path)
                                except Exception as e:
                                    logger.warning(f"读取现有meme文件失败，创建新文件: {e}")
                                    df_meme = pd.DataFrame()
                                df_meme = pd.concat([df_meme, pd.DataFrame(meme_data)], ignore_index=True)
                            else:
                                df_meme = pd.DataFrame(meme_data)
                            
                            df_meme.to_excel(meme_path, index=False)
                            logger.info(f"成功保存 {len(meme_data)} 条meme数据到Excel: {meme_path}")
                        except Exception as e:
                            logger.error(f"保存meme数据时出错: {e}")
                            logger.exception(e)
                    
                    # 处理Twitter搜索
                    if search_terms:
                        logger.info(f"找到搜索词: {search_terms}")
                        twitter_results = []
                        
                        for term in search_terms:
                            # 添加重试机制
                            max_retries = 3
                            retry_delay = 60  # 遇到速率限制时等待60秒
                            
                            for attempt in range(max_retries):
                                try:
                                    import twitter_api
                                    results = await twitter_api.search_tweets(term)
                                    
                                    # 检查是否遇到速率限制
                                    if isinstance(results, dict) and 'errors' in results:
                                        for error in results.get('errors', []):
                                            if error.get('code') == 88:  # Rate limit exceeded
                                                if attempt < max_retries - 1:
                                                    logger.warning(f"遇到速率限制，等待 {retry_delay} 秒后重试 ({attempt + 1}/{max_retries})")
                                                    await asyncio.sleep(retry_delay)
                                                    continue
                                                else:
                                                    logger.error("达到最大重试次数，跳过此搜索词")
                                                    break
                                    
                                    if results and isinstance(results, dict):
                                        tweets = results.get('tweets', [])
                                        if tweets:
                                            logger.info(f"搜索完成 '{term}': 找到 {len(tweets)} 条推文")
                                            
                                            # 只取前10条推文
                                            for tweet in list(tweets)[:10]:
                                                tweet_row = {
                                                    '搜索时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                    '搜索关键词': term,
                                                    '推文ID': tweet.get('id', ''),
                                                    '发推时间': tweet.get('created_at', ''),
                                                    '作者': tweet.get('author', {}).get('username', ''),
                                                    '推文内容': tweet.get('text', ''),
                                                    '转推数': tweet.get('retweet_count', 0),
                                                    '点赞数': tweet.get('like_count', 0),
                                                    '回复数': tweet.get('reply_count', 0),
                                                    '推文URL': f"https://twitter.com/i/web/status/{tweet.get('id', '')}"
                                                }
                                                twitter_results.append(tweet_row)
                                            break
                                        else:
                                            logger.info(f"搜索词 '{term}' 未找到任何推文")
                                            break
                                    else:
                                        logger.warning(f"搜索词 '{term}' 返回的数据格式不正确: {results}")
                                        break
                                        
                                except Exception as e:
                                    if attempt < max_retries - 1:
                                        logger.error(f"搜索出错，将在 {retry_delay} 秒后重试 ({attempt + 1}/{max_retries}): {str(e)}")
                                        await asyncio.sleep(retry_delay)
                                    else:
                                        logger.error(f"达到最大重试次数，搜索失败: {str(e)}")
                            
                            # 在处理下一个搜索词之前等待一小段时间
                            await asyncio.sleep(5)
                        
                        # 保存Twitter搜索结果
                        if twitter_results:
                            try:
                                if twitter_results_path.exists():
                                    df_twitter = pd.read_excel(twitter_results_path)
                                    df_twitter = pd.concat([df_twitter, pd.DataFrame(twitter_results)], ignore_index=True)
                                else:
                                    df_twitter = pd.DataFrame(twitter_results)
                                
                                df_twitter.to_excel(twitter_results_path, index=False)
                                logger.info(f"成功保存 {len(twitter_results)} 条推文到Excel")
                            except Exception as e:
                                logger.error(f"保存Twitter结果时出错: {e}")
                                logger.exception(e)
                    
                except Exception as e:
                    logger.error(f"保存Excel或搜索Twitter时出错: {str(e)}")
                    logger.exception(e)

        except Exception as e:
            logger.error(f"处理消息时出错: {str(e)}")
            logger.exception(e)

    # 不同频道的处理方法
    async def handle_channel_1(self, message):
        """处理频道1的消息"""
        logger.info("正在处理频道1的消息")
        # 这里添加特定的处理逻辑
        # 例如：提取特定信息、转发到其他地方等
        
    async def handle_channel_2(self, message):
        """处理频道2的消息"""
        logger.info("正在处理频道2的消息")
        # 这里添加特定的处理逻辑
        
    async def handle_general_channel(self, message):
        """处理general频道的消息"""
        logger.info("正在处理general频道的消息")
        # 这里添加特定的处理逻辑
        
    async def handle_other_channels(self, message):
        """处理其他频道的消息"""
        logger.info("正在处理其他频道的消息")
        # 这里添加默认的处理逻辑

def main():
    try:
        # 创建并运行客户端
        logger.info("正在启动Discord监控...")
        client = SimpleDiscordMonitor()
        
        # 添加信号处理
        import signal
        def signal_handler(sig, frame):
            logger.info("正在关闭客户端...")
            asyncio.create_task(client.close())
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("开始运行客户端...")
        client.run(client.config['token'])
    except discord.LoginFailure:
        logger.error("登录失败！请检查token是否正确")
    except Exception as e:
        logger.error(f"运行时发生错误: {str(e)}")
        logger.exception(e)  # 打印完整的错误堆栈
        raise

if __name__ == '__main__':
    main()

# 在代码的适当位置添加这个测试
test_tweet = {
    '搜索时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    '搜索关键词': 'test',
    '推文ID': '123456',
    '发推时间': '2024-02-24 00:00:00',
    '作者': 'test_user',
    '推文内容': 'This is a test tweet',
    '转推数': 0,
    '点赞数': 0,
    '回复数': 0,
    '推文URL': 'https://twitter.com/i/web/status/123456'
}
twitter_results = [test_tweet]

# 测试保存功能
if twitter_results:
    twitter_results_path = Path('data/twitter_results.xlsx')
    twitter_results_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df_twitter = pd.DataFrame(twitter_results)
        df_twitter.to_excel(twitter_results_path, index=False)
        logger.info(f"测试：成功保存测试推文到Excel: {twitter_results_path}")
    except Exception as e:
        logger.error(f"测试：保存推文失败: {e}")
        logger.exception(e)