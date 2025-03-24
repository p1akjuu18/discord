import json
import time
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from datetime import datetime, timedelta
import logging
from typing import Optional, Set
import pandas as pd
import os
import sys
from Twitter_historyaianalysis import SiliconFlowClient
from queue import Queue
import threading
from tweet_metrics import TweetMetrics  # 导入TweetMetrics类
from feishu_bot import FeishuBot  # 添加这行导入

# 创建数据保存目录
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 配置日志同时输出到文件和控制台
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 移除所有已存在的处理器
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# 文件处理器 - 使用utf-8编码
file_handler = logging.FileHandler(os.path.join(DATA_DIR, 'feishu_messages.log'), encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.INFO)  # 只记录INFO级别以上的日志
logger.addHandler(file_handler)

# 控制台处理器 - 使用utf-8编码
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler.setLevel(logging.INFO)  # 只记录INFO级别以上的日志
logger.addHandler(console_handler)

# 设置lark的日志级别为WARNING，减少API调用的debug信息
lark.logger.setLevel(logging.WARNING)

class FeishuMessageMonitor:
    def __init__(self, app_id: str, app_secret: str, deepseek_api_key: str):
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
        self.processed_msgs: Set[str] = set()  # 用于存储已处理的消息ID
        self.excel_file = os.path.join(DATA_DIR, 'message_history.xlsx')
        self.analyzed_file = os.path.join(DATA_DIR, 'analyzed_messages.xlsx')
        self.last_timestamp = int(time.time() * 1000)  # 添加时间戳记录
        self.deepseek_client = SiliconFlowClient(deepseek_api_key)
        self.metrics_queue = Queue()  # 创建队列存储需要获取互动数据的消息
        self.tweet_metrics = TweetMetrics("your_api_key")  # 初始化TweetMetrics
        # 启动互动数据采集线程
        self.metrics_thread = threading.Thread(target=self._process_metrics_queue, daemon=True)
        self.metrics_thread.start()
        self.feishu_bot = FeishuBot(app_id, app_secret)  # 添加飞书机器人实例
        self.alert_chat_id = "oc_24a1bdb222fc850d0049b41022acec47"  # 添加目标群聊ID
        
    def monitor_messages(self, chat_id: str, interval: int = 5):
        """
        实时监控消息
        :param chat_id: 群聊ID
        :param interval: 轮询间隔（秒）
        """
        logging.info(f"开始监控群聊 {chat_id}")
        logging.info(f"监控间隔设置为 {interval} 秒")
        
        while True:
            try:
                # 获取最新消息
                messages = self._get_latest_messages(chat_id)
                if messages:
                    self._handle_new_messages(messages)
                
                time.sleep(interval)
                
            except Exception as e:
                logging.error(f"监控过程发生错误: {str(e)}")
                time.sleep(interval)
                
    def _get_latest_messages(self, chat_id: str) -> Optional[list]:
        """获取最新消息"""
        try:
            # 增加页面大小以确保不会遗漏消息
            request = ListMessageRequest.builder() \
                .container_id_type("chat") \
                .container_id(chat_id) \
                .sort_type("ByCreateTimeDesc") \
                .page_size(30) \
                .build()

            response = self.client.im.v1.message.list(request)

            if not response.success():
                logging.error(
                    f"获取消息失败 - 错误码: {response.code}, "
                    f"消息: {response.msg}, "
                    f"日志ID: {response.get_log_id()}"
                )
                return None

            # 只返回上次检查时间之后的新消息
            new_messages = []
            for msg in response.data.items:
                msg_timestamp = int(str(msg.create_time))
                if msg_timestamp > self.last_timestamp:
                    new_messages.append(msg)
            
            # 更新最后处理的时间戳
            if new_messages:
                self.last_timestamp = int(str(new_messages[0].create_time))
                
            return new_messages

        except Exception as e:
            logging.error(f"获取消息时发生错误: {str(e)}")
            return None

    def _save_to_excel(self, message_data: dict):
        """保存消息到Excel文件"""
        try:
            # 如果文件存在，读取现有数据
            if os.path.exists(self.excel_file):
                df_existing = pd.read_excel(self.excel_file)
                df_new = pd.DataFrame([message_data])
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = pd.DataFrame([message_data])
            
            # 保存到Excel，包含表头
            df_combined.to_excel(self.excel_file, index=False, engine='openpyxl')
            
        except Exception as e:
            logging.error(f"保存消息到Excel时发生错误: {str(e)}")
            
    def _process_metrics_queue(self):
        """处理互动数据采集队列的线程"""
        while True:
            try:
                # 从队列中获取消息数据
                message_data, create_time = self.metrics_queue.get()
                current_time = datetime.now()
                msg_create_time = datetime.strptime(create_time, '%Y-%m-%d %H:%M:%S')
                
                # 计算时间差（分钟）
                time_diff = (current_time - msg_create_time).total_seconds() / 60
                
                if time_diff < 3:
                    # 如果还不到3分钟，重新放入队列并等待
                    self.metrics_queue.put((message_data, create_time))
                    time.sleep(10)  # 等待10秒后再检查
                    continue
                    
                elif time_diff >= 3 and time_diff < 10:
                    # 到达3分钟，获取3分钟数据
                    if not self._has_metrics(message_data['message_id'], '3min'):
                        logging.info(f"获取3分钟数据: {message_data['tweet_link']}")
                        metrics_3min = self.tweet_metrics.get_tweet_metrics(message_data['tweet_link'])
                        if metrics_3min:
                            self._update_metrics(message_data['message_id'], metrics_3min, '3min')
                            logging.info(f"3分钟数据更新成功: {metrics_3min}")
                        else:
                            logging.error("获取3分钟数据失败")
                    
                    # 重新放入队列等待10分钟
                    self.metrics_queue.put((message_data, create_time))
                    time.sleep(10)
                    
                else:
                    # 到达10分钟，获取10分钟数据
                    if not self._has_metrics(message_data['message_id'], '10min'):
                        logging.info(f"获取10分钟数据: {message_data['tweet_link']}")
                        metrics_10min = self.tweet_metrics.get_tweet_metrics(message_data['tweet_link'])
                        if metrics_10min:
                            self._update_metrics(message_data['message_id'], metrics_10min, '10min')
                            logging.info(f"10分钟数据更新成功: {metrics_10min}")
                        else:
                            logging.error("获取10分钟数据失败")
                    # 处理完成，不再放回队列
                
                time.sleep(1)  # 避免过于频繁的检查
                
            except Exception as e:
                logging.error(f"处理互动数据时发生错误: {str(e)}")
                time.sleep(5)  # 发生错误时等待更长时间

    def _has_metrics(self, message_id: str, time_point: str) -> bool:
        """检查是否已经有对应时间点的数据"""
        try:
            if os.path.exists(self.excel_file):
                df = pd.read_excel(self.excel_file)
                mask = df['message_id'] == message_id
                if mask.any():
                    # 检查是否存在任意一个该时间点的指标
                    metrics_columns = [f"{metric}_{time_point}" 
                                    for metric in ['likes', 'retweets', 'replies', 'quotes']]
                    return any(df.loc[mask, col].notna().any() for col in metrics_columns)
            return False
        except Exception:
            return False

    def _update_metrics(self, message_id: str, metrics: dict, time_point: str):
        """更新消息的互动数据"""
        try:
            if os.path.exists(self.excel_file):
                df = pd.read_excel(self.excel_file)
                
                mask = df['message_id'] == message_id
                if mask.any():
                    # 更新互动数据
                    for metric_type, value in metrics.items():
                        column_name = f"{metric_type}_{time_point}"
                        df.loc[mask, column_name] = value
                    
                    # 如果是3分钟数据，检查互动总量
                    if time_point == '3min':
                        total_interactions = sum(metrics.values())  # 计算总互动量
                        if total_interactions > 100:  # 第一个触发条件：3分钟内总互动量超过100
                            self._send_alert(df.loc[mask].iloc[0], metrics, total_interactions, "3分钟")
                    
                    # 如果是10分钟数据，检查增长率
                    elif time_point == '10min':
                        total_10min = sum(metrics.values())
                        
                        # 获取3分钟的数据
                        metrics_3min = {}
                        for metric_type in ['likes', 'retweets', 'replies', 'quotes']:
                            col_3min = f"{metric_type}_3min"
                            if col_3min in df.columns:
                                metrics_3min[metric_type] = df.loc[mask, col_3min].iloc[0]
                        
                        # 如果有3分钟数据，计算增长率
                        if metrics_3min and all(isinstance(v, (int, float)) for v in metrics_3min.values()):
                            total_3min = sum(metrics_3min.values())
                            
                            if total_3min > 0:
                                growth_rate = total_10min / total_3min
                                
                                # 第二个触发条件：10分钟总量超过300且是3分钟数据的3倍以上
                                if total_10min > 300 and growth_rate >= 3:
                                    growth_metrics = {
                                        metric: metrics[metric] - metrics_3min.get(metric, 0)
                                        for metric in metrics
                                    }
                                    
                                    self._send_alert(
                                        df.loc[mask].iloc[0], 
                                        metrics,
                                        total_10min,
                                        "10分钟",
                                        growth_rate=growth_rate,
                                        growth_metrics=growth_metrics
                                    )
                    
                    # 保存更新后的数据
                    df.to_excel(self.excel_file, index=False, engine='openpyxl')
                    logging.info(f"已更新消息 {message_id} 的{time_point}互动数据")
                else:
                    logging.warning(f"未找到消息ID {message_id}，无法更新互动数据")
            else:
                logging.warning(f"Excel文件 {self.excel_file} 不存在，无法更新互动数据")
        
        except Exception as e:
            logging.error(f"更新互动数据时发生错误: {str(e)}")

    def _send_alert(self, row_data, metrics, total_interactions, time_point, growth_rate=None, growth_metrics=None):
        """发送高互动量提醒"""
        try:
            # 构造基础消息
            alert_msg = (
                f"🔥 高互动推文提醒\n\n"
                f"{time_point}内总互动量: {total_interactions}\n"
                f"原文: {row_data['content']}\n"
                f"链接: {row_data['tweet_link']}\n\n"
                f"详细数据:\n"
                f"👍 点赞: {metrics.get('likes', 0)}\n"
                f"🔄 转发: {metrics.get('retweets', 0)}\n"
                f"💬 评论: {metrics.get('replies', 0)}\n"
                f"📝 引用: {metrics.get('quotes', 0)}"
            )
            
            # 如果有增长率数据，添加增长信息
            if growth_rate and growth_metrics:
                growth_info = (
                    f"\n\n📈 增长数据:\n"
                    f"增长倍数: {growth_rate:.1f}x\n"
                    f"新增点赞: +{growth_metrics.get('likes', 0)}\n"
                    f"新增转发: +{growth_metrics.get('retweets', 0)}\n"
                    f"新增评论: +{growth_metrics.get('replies', 0)}\n"
                    f"新增引用: +{growth_metrics.get('quotes', 0)}"
                )
                alert_msg += growth_info
            
            # 发送到飞书群
            self.feishu_bot.send_message(self.alert_chat_id, alert_msg)
            logging.info(f"已发送高互动量提醒，消息ID: {row_data['message_id']}")
            
        except Exception as e:
            logging.error(f"发送提醒消息时发生错误: {str(e)}")

    def _handle_new_messages(self, messages: list):
        """处理新消息"""
        analyzed_data = []
        
        for msg in messages:
            if msg.message_id in self.processed_msgs:
                continue
                
            try:
                content = self._get_message_content(msg)
                
                # 添加新的消息解析逻辑
                kol_name, tweet_content, tweet_link, group_type = self.deepseek_client.process_post(content)
                
                # 处理消息
                message_data = {
                    'message_id': msg.message_id,
                    'create_time': datetime.fromtimestamp(
                        int(str(msg.create_time))/1000
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'sender': self._get_sender_info(msg.sender),
                    'msg_type': msg.msg_type,
                    'content': content,
                    'kol_name': kol_name,
                    'tweet_content': tweet_content,
                    'tweet_link': tweet_link,
                    'group_type': group_type
                }
                
                # 如果有推文链接，立即获取一次数据
                if tweet_link:
                    logging.info(f"立即获取推文数据: {tweet_link}")
                    initial_metrics = self.tweet_metrics.get_tweet_metrics(tweet_link, 'initial')
                    if initial_metrics:
                        self._update_metrics(message_data['message_id'], initial_metrics, 'initial')
                        # 检查初始互动量
                        total_interactions = sum(initial_metrics.values())
                        if total_interactions > 100:
                            self._send_alert(message_data, initial_metrics, total_interactions, "初始数据")
                
                # 记录新消息
                logging.info("=== 新消息 ===")
                for key, value in message_data.items():
                    logging.info(f"{key}: {value}")
                logging.info("=============")
                
                # 保存消息到Excel
                self._save_to_excel(message_data)
                
                # 使用DeepSeek API分析消息
                if msg.msg_type == "text" and tweet_content:  # 修改条件，只分析有效的推文内容
                    max_retries = 3
                    retry_count = 0
                    
                    while retry_count < max_retries:
                        try:
                            # 使用解析后的tweet_content进行分析
                            summary, category, asset, sentiment, tags = self.deepseek_client.analyze_tweet(tweet_content)
                            
                            # 检查返回结果是否有效
                            empty_fields = []
                            if not summary or summary.strip() == "":
                                empty_fields.append("summary")
                            if not category or category.strip() == "":
                                empty_fields.append("category")
                            if not sentiment or sentiment.strip() == "":
                                empty_fields.append("sentiment")
                            
                            if len(empty_fields) >= 2:
                                retry_count += 1
                                if retry_count < max_retries:
                                    logging.warning(f"API返回结果不完整 ({', '.join(empty_fields)}为空)，第{retry_count}次重试...")
                                    time.sleep(2)
                                    continue
                                else:
                                    logging.error(f"达到最大重试次数，使用最后一次的返回结果")
                            
                            analyzed_message = {
                                'Message_ID': message_data['message_id'],
                                'Content': message_data['content'],
                                'Create_Time': message_data['create_time'],
                                'Sender': message_data['sender'],
                                'KOL_Name': kol_name,  # 添加KOL名称
                                'Tweet_Content': tweet_content,  # 添加解析后的推文内容
                                'Tweet_Link': tweet_link,  # 添加推文链接
                                'Group_Type': group_type,  # 添加分组类型
                                'Summary': summary,
                                'Category': category,
                                'Asset': asset,
                                'Sentiment': sentiment,
                                'Tags': tags
                            }
                            analyzed_data.append(analyzed_message)
                            
                            # 保存分析结果
                            if os.path.exists(self.analyzed_file):
                                df_existing = pd.read_excel(self.analyzed_file)
                                df_new = pd.DataFrame([analyzed_message])
                                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                            else:
                                df_combined = pd.DataFrame([analyzed_message])
                            
                            df_combined.to_excel(self.analyzed_file, index=False, engine='openpyxl')
                            logging.info(f"消息分析结果已保存到: {self.analyzed_file}")
                            break
                            
                        except Exception as e:
                            retry_count += 1
                            if retry_count < max_retries:
                                logging.error(f"分析消息时发生错误: {str(e)}，第{retry_count}次重试...")
                                time.sleep(2)
                            else:
                                logging.error(f"分析消息失败，达到最大重试次数: {str(e)}")
                
                # 确认文件是否存在
                if os.path.exists(self.excel_file):
                    logging.info(f"消息已保存到文件: {self.excel_file}")
                else:
                    logging.warning("警告：文件未能成功创建！")
                
                # 将消息ID添加到已处理集合
                self.processed_msgs.add(msg.message_id)
                
                if len(self.processed_msgs) > 1000:
                    self.processed_msgs = set(list(self.processed_msgs)[-500:])
                
                # 如果消息包含推文链接，加入互动数据采集队列继续跟踪
                if tweet_link:
                    self.metrics_queue.put((message_data, message_data['create_time']))
                
            except Exception as e:
                logging.error(f"处理消息时发生错误: {str(e)}")

    def _get_sender_info(self, sender):
        """获取发送者信息"""
        if not sender:
            return "未知用户"
        
        for attr in ['id', 'user_id', 'union_id']:
            if hasattr(sender, attr):
                return getattr(sender, attr)
        
        return str(sender)

    def _get_message_content(self, msg):
        """获取消息内容"""
        if not msg.body:
            return "无内容"
            
        if msg.msg_type == "text":
            return msg.body.content
        elif msg.msg_type == "system":
            try:
                content_dict = json.loads(msg.body.content)
                return json.dumps(content_dict, ensure_ascii=False)
            except:
                return msg.body.content
        else:
            return f"[{msg.msg_type}] {msg.body.content}"

def main():
    # 配置信息
    APP_ID = "cli_a736cea2ff78100d"
    APP_SECRET = "C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS"
    CHAT_ID = "oc_67aebe4cd9e8dbdccdf292f11eb02e1c"
    DEEPSEEK_API_KEY = "sk-iqpehavwylkwnmxcsbantqhdwceqqhqmvsvlsbtoaegxxaze"
    
    # 创建监控器实例
    monitor = FeishuMessageMonitor(APP_ID, APP_SECRET, DEEPSEEK_API_KEY)
    
    # 开始监控消息
    monitor.monitor_messages(CHAT_ID, interval=5)

if __name__ == "__main__":
    main() 