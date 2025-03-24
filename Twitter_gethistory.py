import json
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from datetime import datetime
import logging
import pandas as pd
import os
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='feishu_messages.log'
)

class FeishuMessageHandler:
    def __init__(self, app_id, app_secret):
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
    
    def get_messages(self, chat_id, page_size=20, sort_type="ByCreateTimeDesc", page_token=None):
        """
        获取群聊消息
        :param chat_id: 群聊ID
        :param page_size: 每页消息数量
        :param sort_type: 排序方式 (ByCreateTimeAsc/ByCreateTimeDesc)
        :param page_token: 分页标记，用于获取下一页数据
        :return: 消息列表或None
        """
        try:
            request = ListMessageRequest.builder() \
                .container_id_type("chat") \
                .container_id(chat_id) \
                .sort_type(sort_type) \
                .page_size(page_size)
            
            if page_token:
                request.page_token(page_token)
            
            request = request.build()
            response = self.client.im.v1.message.list(request)

            if not response.success():
                logging.error(
                    f"获取消息失败 - 错误码: {response.code}, "
                    f"消息: {response.msg}, "
                    f"日志ID: {response.get_log_id()}"
                )
                return None, None

            return response.data.items, response.data.page_token

        except Exception as e:
            logging.error(f"获取消息时发生错误: {str(e)}")
            return None, None

    def process_messages(self, messages):
        """
        处理消息列表
        :param messages: 消息列表
        :return: 处理后的消息字典列表
        """
        if not messages:
            return []

        processed_messages = []
        for msg in messages:
            try:
                message_data = {
                    'message_id': msg.message_id,
                    'create_time': datetime.fromtimestamp(
                        int(str(msg.create_time))/1000
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'sender': self._get_sender_info(msg.sender),
                    'msg_type': msg.msg_type,
                    'content': self._get_message_content(msg)
                }
                processed_messages.append(message_data)
            except Exception as e:
                logging.error(f"处理消息时发生错误: {str(e)}")
                continue

        return processed_messages

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
    APP_ID = "cli_a736cea2ff78100d"
    APP_SECRET = "C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS"
    CHAT_ID = "oc_67aebe4cd9e8dbdccdf292f11eb02e1c"
    
    # 设置起始时间
    start_time = datetime.strptime("2025-02-27 2:55:00", "%Y-%m-%d %H:%M:%S")
    
    print("开始获取飞书消息...")
    
    handler = FeishuMessageHandler(APP_ID, APP_SECRET)
    
    all_messages = []
    page_token = None
    
    try:
        while True:
            messages, next_page_token = handler.get_messages(
                CHAT_ID, 
                page_size=20,
                sort_type="ByCreateTimeDesc",
                page_token=page_token
            )
            
            if not messages:
                break
            
            processed_messages = handler.process_messages(messages)
            
            # 过滤出指定时间之后的消息
            filtered_messages = [
                msg for msg in processed_messages 
                if datetime.strptime(msg['create_time'], "%Y-%m-%d %H:%M:%S") >= start_time
            ]
            
            all_messages.extend(filtered_messages)
            
            print(f"已获取 {len(all_messages)} 条消息")
            
            # 如果当前批次的最后一条消息的时间早于起始时间，说明后面的消息都更早，可以停止获取
            if processed_messages:
                last_msg_time = datetime.strptime(processed_messages[-1]['create_time'], "%Y-%m-%d %H:%M:%S")
                if last_msg_time < start_time:
                    print("已获取所有指定时间之后的消息")
                    break
            
            if not next_page_token:
                print("已获取所有消息")
                break
                
            page_token = next_page_token
            time.sleep(1)  # 添加短暂延迟，避免请求过快
        
        # 将消息转换为DataFrame并保存为Excel
        if all_messages:
            print(f"\n开始保存消息，共 {len(all_messages)} 条...")
            df = pd.DataFrame(all_messages)
            # 按时间排序
            df = df.sort_values('create_time', ascending=True)
            # 保存为Excel文件
            excel_file = "chat_history.xlsx"
            df.to_excel(excel_file, index=False, engine='openpyxl')
            print(f"消息已保存到: {os.path.abspath(excel_file)}")
            print("\n消息示例:")
            print(df.head())  # 显示前5条消息
                
    except Exception as e:
        logging.error(f"程序运行出错: {str(e)}")
        print(f"错误详情: {str(e)}")
        
        # 即使出错也保存已获取的消息
        if all_messages:
            df = pd.DataFrame(all_messages)
            df = df.sort_values('create_time', ascending=True)
            df.to_excel("chat_history.xlsx", index=False, engine='openpyxl')
            print(f"已保存 {len(df)} 条消息到 chat_history.xlsx")

if __name__ == "__main__":
    main() 