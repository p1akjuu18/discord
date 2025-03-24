import json
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import os
from dotenv import load_dotenv
import requests
import logging
import time

class FeishuBot:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = None
        self.token_expire_time = 0
        
    def _get_tenant_access_token(self):
        """获取tenant_access_token"""
        import time
        import requests
        
        # 检查现有token是否过期
        current_time = int(time.time())
        if self.tenant_access_token and current_time < self.token_expire_time:
            return self.tenant_access_token
            
        try:
            url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
            headers = {
                "Content-Type": "application/json"
            }
            data = {
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
            
            response = requests.post(url, json=data, headers=headers)
            response.raise_for_status()  # 确保请求成功
            
            result = response.json()
            if result.get("code") == 0:
                self.tenant_access_token = result.get("tenant_access_token")
                self.token_expire_time = current_time + result.get("expire", 7200)
                return self.tenant_access_token
            else:
                print(f"获取tenant_access_token失败: {result}")
                return None
                
        except Exception as e:
            print(f"获取tenant_access_token时出错: {str(e)}")
            return None
            
    def send_message(self, receive_id, content, use_webhook=False):
        """
        发送消息到飞书
        
        参数:
            receive_id (str): 接收消息的聊天ID
            content (str): 消息内容，markdown格式
            use_webhook (bool): 是否使用webhook发送，默认False
            
        返回:
            bool: 发送是否成功
        """
        import requests
        import json
        
        try:
            # 获取access token
            token = self._get_tenant_access_token()
            if not token and not use_webhook:
                print("未能获取tenant_access_token，无法发送消息")
                return False
                
            if use_webhook:
                # 使用Webhook发送
                # 这里实现webhook逻辑
                return False  # 暂不实现
            else:
                # 使用API发送
                url = "https://open.feishu.cn/open-apis/im/v1/messages"
                
                # 检查receive_id是否包含特定前缀
                if not receive_id.startswith("oc_") and not receive_id.startswith("chat_"):
                    receive_id = f"chat_{receive_id}"
                
                # 打印详细信息用于调试  
                print(f"发送消息到: {receive_id}")
                print(f"使用Token: {token[:10]}...")
                
                params = {"receive_id_type": "chat_id"}
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                }
                
                # 构建消息内容
                msg_data = {
                    "receive_id": receive_id,
                    "content": json.dumps({
                        "text": content
                    }),
                    "msg_type": "text"
                }
                
                # 如果是markdown格式，修改消息类型
                if content.startswith("#") or "**" in content:
                    msg_data["msg_type"] = "interactive"
                    msg_data["content"] = json.dumps({
                        "elements": [{
                            "tag": "markdown",
                            "content": content
                        }]
                    })
                
                print(f"发送数据: {msg_data}")  # 调试用
                
                response = requests.post(url, json=msg_data, headers=headers, params=params)
                result = response.json()
                
                if response.status_code == 200 and result.get("code") == 0:
                    print("消息发送成功")
                    return True
                else:
                    print(f"消息发送失败: {result}")
                    return False
                    
        except Exception as e:
            print(f"发送消息时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def get_chat_list(self):
        """获取机器人所在的群列表"""
        try:
            # 构造请求对象
            request = ListChatRequest.builder().build()
            
            # 发起请求
            response = self.client.im.v1.chat.list(request)
            
            if not response.success():
                lark.logger.error(
                    f"获取群列表失败，错误码: {response.code}, "
                    f"错误信息: {response.msg}"
                )
                return None
                
            return response.data.items
            
        except Exception as e:
            lark.logger.error(f"获取群列表时发生错误: {str(e)}")
            return None

    def _get_access_token(self):
        """获取飞书访问令牌（新版API）"""
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        # 新版API端点
        url = f"{self.base_url}/auth/v3/app_access_token/internal"
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                self.access_token = result.get("app_access_token")
                expires_in = result.get("expire") - 300
                self.token_expires_at = time.time() + expires_in
                return self.access_token
            else:
                logger.error(f"获取飞书访问令牌失败: {result}")
                return None
        except Exception as e:
            logger.error(f"获取飞书访问令牌异常: {str(e)}")
            return None

def main():
    bot = FeishuBot()
    
    # 使用固定的群ID
    chat_id = "oc_24a1bdb222fc850d0049b41022acec47"
    message = "这是一条测试消息"
    
    # 使用webhook方式发送
    bot.send_message(chat_id, message, use_webhook=True)
    
    # 使用API方式发送
    bot.send_message(chat_id, message, use_webhook=False)

if __name__ == "__main__":
    main() 