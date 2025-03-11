from flask import Flask, request, jsonify
from datetime import datetime
import json
import traceback

app = Flask(__name__)

# 飞书应用配置
APP_ID = "cli_a736cea2ff78100d"
APP_SECRET = "C9FsC6CnJz3CLf0PEz0NQewkuH6uvCdS"

def save_message(message_data):
    """保存消息到文件"""
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if isinstance(message_data, dict):
            # 提取消息内容
            msg_type = message_data.get('msg_type', '')
            content = message_data.get('content', {})
            
            if msg_type == 'text':
                try:
                    content_dict = json.loads(content)
                    message = content_dict.get('text', '')
                except:
                    message = content
            else:
                message = json.dumps(message_data, ensure_ascii=False, indent=2)
        else:
            message = str(message_data)
            
        full_message = f"\n[{current_time}]\n{message}\n"
        
        # 保存到文件
        with open('received_messages.txt', 'a', encoding='utf-8') as f:
            f.write(full_message)
            
        print("\n收到新消息:", full_message)
        return full_message
        
    except Exception as e:
        error_msg = f"保存消息失败: {e}\n{traceback.format_exc()}"
        print(error_msg)
        return error_msg

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        print("\n=== 收到消息 ===")
        print("Headers:", dict(request.headers))
        
        # 获取原始数据
        raw_data = request.get_data(as_text=True)
        print("原始数据:", raw_data)
        
        # 解析消息
        data = request.get_json(force=True)
        print("消息内容:", json.dumps(data, ensure_ascii=False, indent=2))
        
        # 处理事件订阅的验证请求
        if data.get('type') == 'url_verification':
            return {
                "challenge": data.get('challenge')
            }
        
        # 保存消息
        message = save_message(data)
        
        return {
            "code": 0,
            "msg": "success"
        }
        
    except Exception as e:
        error_msg = f"处理消息时出错: {e}\n{traceback.format_exc()}"
        print("\n错误:")
        print(error_msg)
        return {
            "code": 500,
            "msg": str(e)
        }, 500

if __name__ == '__main__':
    PORT = 5000
    print(f"\n=== 飞书事件订阅服务器启动 ===")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"应用 ID: {APP_ID}")
    print(f"接收地址: http://127.0.0.1:{PORT}/webhook")
    print("\n等待接收消息...\n")
    
    try:
        app.run(host='127.0.0.1', port=PORT)
    except Exception as e:
        print(f"\n启动服务器失败: {e}\n{traceback.format_exc()}") 