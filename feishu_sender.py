import requests
import json
from datetime import datetime

def send_feishu_message(text):
    """发送消息到飞书并保存"""
    webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/c7fe919c-6bba-4887-938c-1bd478211400"
    
    try:
        # 1. 保存消息
        save_path = "sent_messages.txt"
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        message_line = f"[{current_time}] {text}\n"
        with open(save_path, 'a', encoding='utf-8') as f:
            f.write(message_line)
            
        # 2. 发送消息
        data = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()  # 检查响应状态
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"发送消息到飞书失败: {e}")
        raise
    except Exception as e:
        print(f"处理消息时出错: {e}")
        raise

# 测试
if __name__ == "__main__":
    result = send_feishu_message("测试消息")
    print("发送结果:", result) 