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
from SimpleDiscordMonitor_v2 import SimpleDiscordMonitor, Config, SiliconFlowClient

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
    async def setup_http_session(self):
        """设置HTTP会话"""
        try:
            # 修改连接器配置
            connector = ProxyConnector.from_url(
                'http://127.0.0.1:7890',
                ssl=False,  # 只保留这一个SSL相关参数
                force_close=True,
                enable_cleanup_closed=True,
                ttl_dns_cache=300,
                limit=10,
                family=socket.AF_INET,
                rdns=True
                # 移除 verify_ssl 参数
            )
            
            # 设置更长的超时时间
            timeout = aiohttp.ClientTimeout(
                total=300,        # 总超时时间增加到5分钟
                connect=60,       # 连接超时增加到1分钟
                sock_connect=60,  # socket连接超时增加到1分钟
                sock_read=60      # socket读取超时增加到1分钟
            )
            
            logger.info(f"HTTP会话使用代理: http://127.0.0.1:7890")
            
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

    def __init__(self):
        try:
            # 从配置文件加载设置
            with open('config.json', 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            # 确保使用绝对路径，并规范化路径分隔符
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.save_path = os.path.normpath(os.path.join(base_dir, self.config['monitor']['save_path']))
            
            # 确保文件路径有 .json 扩展名
            if not self.save_path.endswith('.json'):
                self.save_path += '.json'
            
            save_dir = os.path.dirname(self.save_path)
            
            # 打印详细的路径信息
            logger.info(f"基础目录: {base_dir}")
            logger.info(f"配置的保存路径: {self.config['monitor']['save_path']}")
            logger.info(f"完整的保存路径: {self.save_path}")
            logger.info(f"保存目录: {save_dir}")
            
            # 检查当前用户和权限
            import getpass
            logger.info(f"当前运行用户: {getpass.getuser()}")
            
            # 创建保存目录
            try:
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                    logger.info(f"已创建目录: {save_dir}")
                
                # Windows系统下不需要修改权限
                if os.name != 'nt':  # 如果不是Windows系统
                    try:
                        os.chmod(save_dir, 0o777)
                        logger.info("已设置目录权限")
                    except Exception as e:
                        logger.warning(f"设置目录权限失败: {e}")
                
                logger.info(f"目录准备完成: {save_dir}")
                
            except Exception as e:
                logger.error(f"处理目录时出错: {str(e)}")
                raise
            
            # 初始化消息列表
            self.messages = []
            
            # 如果文件存在则加载，不存在则创建空文件
            try:
                if os.path.exists(self.save_path):
                    try:
                        with open(self.save_path, 'r', encoding='utf-8') as f:
                            self.messages = json.load(f)
                        logger.info(f"已加载现有消息文件: {self.save_path}")
                    except Exception as e:
                        logger.error(f"读取消息文件失败: {str(e)}")
                        # 如果读取失败，尝试创建新文件
                        with open(self.save_path, 'w', encoding='utf-8') as f:
                            json.dump([], f, ensure_ascii=False, indent=2)
                        logger.info(f"已创建新的消息文件: {self.save_path}")
                else:
                    # 创建新文件
                    with open(self.save_path, 'w', encoding='utf-8') as f:
                        json.dump([], f, ensure_ascii=False, indent=2)
                    logger.info(f"已创建新的消息文件: {self.save_path}")
                    
                    # Windows系统下不需要修改权限
                    if os.name != 'nt':
                        try:
                            os.chmod(self.save_path, 0o666)
                        except Exception as e:
                            logger.warning(f"设置文件权限失败: {e}")
            
            except Exception as e:
                logger.error(f"处理消息文件时出错: {str(e)}")
                raise
            
            # 加载提示词配置
            try:
                with open('prompts.json', 'r', encoding='utf-8') as f:
                    self.prompts = json.load(f)
                logger.info("成功加载 prompts.json")
            except Exception as e:
                logger.error(f"加载 prompts.json 失败: {str(e)}")
                self.prompts = {}
            
            # 修改 discord.py-self 的初始化参数
            super().__init__(
                chunk_guilds_at_startup=False,
                proxy="http://127.0.0.1:7890",  # 保持代理设置
                max_messages=10000,
                heartbeat_timeout=300.0,    # 增加心跳超时到5分钟
                guild_ready_timeout=60.0,   # 增加公会准备超时到1分钟
                assume_unsync_clock=True
            )
            
            logger.info("Discord客户端初始化完成")
            
        except Exception as e:
            logger.error(f"初始化时发生错误: {str(e)}")
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
            # 确保文件路径有 .json 扩展名
            if not self.save_path.endswith('.json'):
                self.save_path += '.json'
            
            # 获取完整的文件路径
            full_path = os.path.abspath(self.save_path)
            save_dir = os.path.dirname(full_path)
            
            # 确保目录存在
            if not os.path.exists(save_dir):
                try:
                    os.makedirs(save_dir)
                    logger.info(f"已创建目录: {save_dir}")
                except Exception as e:
                    logger.error(f"创建目录失败: {str(e)}")
                    raise
            
            # 尝试保存文件
            try:
                # 先尝试创建一个临时文件来测试写入权限
                temp_path = os.path.join(save_dir, 'temp_test.json')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    f.write('{}')
                os.remove(temp_path)
                
                # 如果临时文件测试成功，则保存实际文件
                with open(full_path, 'w', encoding='utf-8') as f:
                    json.dump(self.messages, f, ensure_ascii=False, indent=2)
                
                logger.info(f"消息已成功保存到: {full_path}")
                
            except PermissionError:
                # 如果遇到权限错误，尝试保存到用户目录
                user_path = os.path.expanduser('~/discord_messages.json')
                logger.warning(f"原始路径无权限，尝试保存到用户目录: {user_path}")
                
                with open(user_path, 'w', encoding='utf-8') as f:
                    json.dump(self.messages, f, ensure_ascii=False, indent=2)
                
                # 更新保存路径
                self.save_path = user_path
                logger.info(f"消息已保存到备用位置: {user_path}")
                
        except Exception as e:
            logger.error(f"保存消息时出错: {str(e)}")
            logger.exception(e)
            # 不要在这里继续抛出异常，让程序继续运行

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

            # 检查是否是私信消息
            is_dm = isinstance(message.channel, (discord.DMChannel, discord.PartialMessageable))

            # 记录消息信息
            print("\n" + "=" * 50)
            print(f"收到新消息:")
            print(f"服务器: {message.guild.name if message.guild else '私信'}")
            print(f"频道: {'私信' if is_dm else message.channel.name}")
            print(f"频道ID: {message.channel.id}")
            print(f"发送者: {message.author.name} ({message.author.id})")
            print(f"时间: {message.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 保存消息数据
            message_data = {
                "timestamp": message.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "guild_id": str(message.guild.id) if message.guild else None,
                "guild_name": message.guild.name if message.guild else "私信",
                "channel_id": str(message.channel.id),
                "channel_name": "私信" if is_dm else message.channel.name,
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
                "message_type": "私信" if is_dm else "频道消息"
            }
            
            # 如果是私信，直接保存消息并返回
            if is_dm:
                logger.info("收到私信消息")
                self.messages.append(message_data)
                self.save_messages()
                logger.info("私信消息已保存")
                return
            
            # 测试 API 调用（只处理非私信消息）
            if str(message.channel.id) in self.config['monitor']['channels']:
                logger.info("检测到目标频道消息，准备调用 API")
                
                try:
                    # 初始化 API 客户端
                    from SimpleDiscordMonitor_v2 import SiliconFlowClient
                    api_key = self.config.get('api', {}).get('key', '')  # 从配置中获取 API key
                    
                    if not api_key:
                        logger.error("未找到 API key，请在配置文件中添加")
                        return
                        
                    client = SiliconFlowClient(api_key)
                    
                    # 从 prompts.json 读取提示词
                    try:
                        with open('prompts.json', 'r', encoding='utf-8') as f:
                            prompts = json.load(f)
                        logger.info("成功加载 prompts.json")
                    except Exception as e:
                        logger.error(f"读取 prompts.json 失败: {str(e)}")
                        return
                    
                    # 构建提示词
                    channel_id = str(message.channel.id)
                    prompt = prompts.get(channel_id)
                    
                    if not prompt:
                        logger.warning(f"未在 prompts.json 中找到频道 {channel_id} 的提示词配置")
                        return
                    
                    # 准备消息内容
                    content = message.content
                    if message.embeds:
                        for embed in message.embeds:
                            if embed.description:
                                content += f"\n{embed.description}"
                    
                    # 构建 API 请求
                    messages = [{
                        "role": "user",
                        "content": f"{prompt}\n\n需要分析的内容:\n{content}"
                    }]
                    
                    logger.info("开始调用 API...")
                    logger.info(f"使用提示词: {prompt}")
                    logger.info(f"发送内容: {content}")
                    
                    # 调用 API
                    response = await client.chat_completion(
                        messages=messages,
                        model="deepseek-ai/DeepSeek-V3",
                        max_tokens=1024,
                        temperature=0.7
                    )
                    
                    # 处理响应
                    if response and 'choices' in response:
                        result = response['choices'][0]['message']['content']
                        logger.info("API 调用成功！")
                        logger.info("API 返回结果：")
                        logger.info(result)
                        
                        # 可以在这里添加结果的处理逻辑
                        
                    else:
                        logger.error("API 返回格式错误")
                        logger.error(f"返回内容: {response}")
                    
                except Exception as e:
                    logger.error(f"调用 API 时出错: {str(e)}")
                    logger.exception(e)
                
                finally:
                    # 确保关闭 API 客户端
                    if 'client' in locals():
                        await client.close()
            
            # 保存消息
            self.messages.append(message_data)
            self.save_messages()
            logger.info("消息已保存")
            
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

async def test_api_call():
    """测试 API 调用流程"""
    try:
        logger.info("\n" + "=" * 50)
        logger.info("开始 API 调用测试")
        
        # 初始化 API 客户端
        from SimpleDiscordMonitor_v2 import SiliconFlowClient
        
        # 从配置文件加载 API key
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        api_key = config.get('api', {}).get('key', '')
        
        if not api_key:
            logger.error("未找到 API key，请在 config.json 中添加")
            return
            
        # 创建测试消息内容
        test_content = """
        BTC/USDT
        多单信号
        入场: 84500
        止损: 83000
        目标: 89000
        """
        
        # 从 prompts.json 加载提示词
        with open('prompts.json', 'r', encoding='utf-8') as f:
            prompts = json.load(f)
        
        # 使用测试频道的提示词
        channel_id = "1283359910788202499"  # 测试频道 ID
        prompt = prompts.get(channel_id)
        
        if not prompt:
            logger.error(f"未找到频道 {channel_id} 的提示词")
            return
            
        logger.info(f"使用频道 ID: {channel_id}")
        logger.info(f"使用提示词: {prompt}")
        logger.info(f"测试消息内容: {test_content}")
        
        # 构建 API 请求
        messages = [{
            "role": "user",
            "content": f"{prompt}\n\n需要分析的内容:\n{test_content}"
        }]
        
        # 初始化客户端并调用 API
        client = SiliconFlowClient(api_key)
        logger.info("\n开始调用 API...")
        
        response = await client.chat_completion(
            messages=messages,
            model="deepseek-ai/DeepSeek-V3",
            max_tokens=1024,
            temperature=0.7
        )
        
        # 处理响应
        if response and 'choices' in response:
            result = response['choices'][0]['message']['content']
            logger.info("\nAPI 调用成功！")
            logger.info("API 返回结果：")
            logger.info(result)
        else:
            logger.error("API 返回格式错误")
            logger.error(f"返回内容: {response}")
            
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        logger.exception(e)
    finally:
        if 'client' in locals():
            await client.close()
        logger.info("=" * 50 + "\n")

if __name__ == '__main__':
    # 设置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 运行测试
    asyncio.run(test_api_call())