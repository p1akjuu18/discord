import json
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from datetime import datetime
import traceback

def format_timestamp(timestamp):
    """格式化时间戳"""
    try:
        # 确保时间戳是整数
        ts = int(str(timestamp))
        return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(timestamp)

def get_sender_info(sender):
    """获取发送者信息"""
    try:
        if not sender:
            return "未知用户"
        
        # 获取用户 ID
        if hasattr(sender, 'id'):
            return sender.id
        elif hasattr(sender, 'user_id'):
            return sender.user_id
        elif hasattr(sender, 'union_id'):
            return sender.union_id
        else:
            return str(sender)
    except:
        return "未知用户"

def get_message_content(msg):
    """获取消息内容"""
    try:
        if not msg.body:
            return "无内容"
            
        # 根据消息类型处理
        if msg.msg_type == "text":
            return msg.body.content
        elif msg.msg_type == "system":
            # 系统消息可能有特殊格式
            try:
                content_dict = json.loads(msg.body.content)
                return json.dumps(content_dict, ensure_ascii=False, indent=2)
            except:
                return msg.body.content
        else:
            # 其他类型消息
            return f"[{msg.msg_type}] {msg.body.content}"
            
    except Exception as e:
        print(f"获取消息内容失败: {e}")
        return "无法获取消息内容"

def save_messages(messages):
    """保存消息到文件"""
    try:
        with open('chat_history.txt', 'a', encoding='utf-8') as f:
            f.write(f"\n=== 消息记录 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            
            for msg in messages:
                f.write(f"\n消息ID: {msg.message_id}")
                f.write(f"\n时间: {format_timestamp(msg.create_time)}")
                f.write(f"\n发送者: {get_sender_info(msg.sender)}")
                f.write(f"\n类型: {msg.msg_type}")
                f.write(f"\n内容: {get_message_content(msg)}")
                f.write("\n---\n")
                
    except Exception as e:
        print(f"保存消息失败: {e}")
        print(f"错误详情: {traceback.format_exc()}")

def get_chat_messages():
    """获取群聊历史消息"""
    try:
        # 创建 client
        client = lark.Client.builder() \
            .app_id("cli_a736cea2ff78100d") \
            .app_secret("C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS") \
            .log_level(lark.LogLevel.DEBUG) \
            .build()

        # 构造请求
        request = ListMessageRequest.builder() \
            .container_id_type("chat") \
            .container_id("oc_67aebe4cd9e8dbdccdf292f11eb02e1c") \
            .sort_type("ByCreateTimeDesc") \
            .page_size(20) \
            .build()

        # 发送请求
        print("正在获取消息...")
        response = client.im.v1.message.list(request)

        # 处理响应
        if not response.success():
            print(f"获取消息失败:")
            print(f"错误码: {response.code}")
            print(f"错误信息: {response.msg}")
            print(f"日志ID: {response.get_log_id()}")
            return

        # 处理消息
        messages = response.data.items
        print(f"\n获取到 {len(messages)} 条消息")
        
        # 保存消息
        save_messages(messages)
        
        # 打印消息
        for msg in messages:
            print("\n---消息详情---")
            print(f"消息ID: {msg.message_id}")
            print(f"时间: {format_timestamp(msg.create_time)}")
            print(f"发送者: {get_sender_info(msg.sender)}")
            print(f"类型: {msg.msg_type}")
            print(f"内容: {get_message_content(msg)}")
            print("------------")

    except Exception as e:
        print(f"发生错误: {e}")
        print(f"错误详情: {traceback.format_exc()}")

if __name__ == "__main__":
    print("=== 飞书消息获取工具 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    get_chat_messages() 