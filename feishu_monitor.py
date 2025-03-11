import json
import time
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from datetime import datetime
import logging
from typing import Optional, Set
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='feishu_messages.log'
)

class FeishuMessageMonitor:
    def __init__(self, app_id: str, app_secret: str):
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
        self.processed_msgs: Set[str] = set()  # 用于存储已处理的消息ID
        self.excel_file = 'message_history.xlsx'
        self.last_timestamp = int(time.time() * 1000)  # 添加时间戳记录
        
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
            
    def _handle_new_messages(self, messages: list):
        """处理新消息"""
        for msg in messages:
            if msg.message_id in self.processed_msgs:
                continue
                
            try:
                # 处理消息
                message_data = {
                    'message_id': msg.message_id,
                    'create_time': datetime.fromtimestamp(
                        int(str(msg.create_time))/1000
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'sender': self._get_sender_info(msg.sender),
                    'msg_type': msg.msg_type,
                    'content': self._get_message_content(msg)
                }
                
                # 记录新消息
                logging.info("=== 新消息 ===")
                for key, value in message_data.items():
                    logging.info(f"{key}: {value}")
                logging.info("=============")
                
                # 保存消息到Excel
                self._save_to_excel(message_data)
                
                # 确认文件是否存在
                if os.path.exists(self.excel_file):
                    logging.info(f"消息已保存到文件: {self.excel_file}")
                else:
                    logging.warning("警告：文件未能成功创建！")
                
                # 将消息ID添加到已处理集合
                self.processed_msgs.add(msg.message_id)
                
                if len(self.processed_msgs) > 1000:
                    self.processed_msgs = set(list(self.processed_msgs)[-500:])
                    
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
    
    # 创建监控器实例
    monitor = FeishuMessageMonitor(APP_ID, APP_SECRET)
    
    # 开始监控消息
    monitor.monitor_messages(CHAT_ID, interval=5)

if __name__ == "__main__":
    main() 