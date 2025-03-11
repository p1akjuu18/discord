import requests
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
import time
from crypto_analysis_manager import CryptoAnalysisManager
from plyer import notification  # 需要先安装: pip install plyer

class AlertChannel:
    def __init__(self):
        # 替换为您的飞书 webhook 地址
        self.lark_webhook = "https://open.larksuite.com/open-apis/bot/v2/hook/xxxxxx"  # 替换为您的webhook

    def send_lark_message(self, message: str) -> bool:
        """发送飞书消息"""
        try:
            data = {
                "msg_type": "text",
                "content": {
                    "text": message
                }
            }
            response = requests.post(self.lark_webhook, json=data)
            return response.status_code == 200
        except Exception as e:
            print(f"飞书推送失败: {e}")
            return False

class CryptoAlertSystem:
    def __init__(self, api_key: str):
        self.analyzer = CryptoAnalysisManager(api_key)
        self.alert_channel = AlertChannel()
        
    def analyze_and_alert(self, tweets: List[str], keyword: str) -> None:
        """分析推文并根据条件发送警报"""
        
        # 进行多维度分析
        narrative_response = self.analyzer.analyze_narrative(tweets)
        risk_response = self.analyzer.analyze_risk(tweets)
        sentiment_response = self.analyzer.analyze_sentiment(tweets)
        
        if not all([narrative_response, risk_response, sentiment_response]):
            print("分析失败")
            return

        # 解析结果
        narrative = narrative_response['choices'][0]['message']['content']
        risk = risk_response['choices'][0]['message']['content']
        sentiment = sentiment_response['choices'][0]['message']['content']

        # 判断是否需要发送警报
        should_alert = self._should_send_alert(narrative, risk, sentiment)
        
        if should_alert:
            # 发送桌面通知
            self._send_desktop_notification(keyword, narrative, risk, sentiment)
            # 同时在控制台打印详细信息
            self._print_detailed_info(keyword, narrative, risk, sentiment)

    def _should_send_alert(self, narrative: str, risk: str, sentiment: str) -> bool:
        """判断是否需要发送警报的逻辑"""
        risk_level = self._extract_risk_level(risk)
        sentiment_score = self._extract_sentiment_score(sentiment)
        
        return (
            risk_level == "高" or 
            sentiment_score >= 8 or 
            sentiment_score <= 3
        )

    def _send_desktop_notification(self, keyword: str, narrative: str, risk: str, sentiment: str):
        """发送桌面通知"""
        title = f"加密货币预警 - {keyword}"
        # 简化的消息内容，因为通知栏空间有限
        message = f"风险等级: {self._extract_risk_level(risk)}\n情绪指数: {self._extract_sentiment_score(sentiment)}"
        
        try:
            notification.notify(
                title=title,
                message=message,
                app_icon=None,  # 可以设置自定义图标
                timeout=10  # 通知显示时间（秒）
            )
        except Exception as e:
            print(f"发送通知失败: {e}")

    def _print_detailed_info(self, keyword: str, narrative: str, risk: str, sentiment: str):
        """在控制台打印详细信息"""
        print("\n" + "="*50)
        print(f"🚨 加密货币预警 - {keyword}")
        print("="*50)
        print("\n📊 叙事分析:")
        print(narrative)
        print("\n⚠️ 风险评估:")
        print(risk)
        print("\n📈 市场情绪:")
        print(sentiment)
        print("\n🕒 时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print("="*50 + "\n")

    def _extract_risk_level(self, risk_text: str) -> str:
        """从风险分析结果中提取风险等级"""
        for line in risk_text.split('\n'):
            if line.startswith('风险等级：'):
                return line.replace('风险等级：', '').strip()
        return "未知"

    def _extract_sentiment_score(self, sentiment_text: str) -> int:
        """从情绪分析结果中提取情绪指数"""
        for line in sentiment_text.split('\n'):
            if line.startswith('情绪指数：'):
                try:
                    return int(line.replace('情绪指数：', '').strip())
                except ValueError:
                    return 5
        return 5

    def _format_alert_message(self, keyword: str, narrative: str, risk: str, sentiment: str) -> str:
        """格式化飞书消息"""
        return f"""
【加密货币分析报告】
关键词：{keyword}
时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

=== 叙事分析 ===
{narrative}

=== 风险评估 ===
{risk}

=== 市场情绪 ===
{sentiment}

#CryptoAlert #{keyword}
"""

    def _send_alerts(self, message: str) -> None:
        """发送到飞书"""
        data = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "加密货币预警通知",
                        "content": [
                            [{
                                "tag": "text",
                                "text": message
                            }]
                        ]
                    }
                }
            }
        }
        self.alert_channel.send_lark_message(data)

def monitor_and_analyze(input_file: str):
    """监控Excel文件并进行分析"""
    api_key = "sk-ijfgzjxmxcbiqzznpusbwgitmwxvkiwyddabfxmapjontfbm"
    alert_system = CryptoAlertSystem(api_key)
    
    # 读取Excel
    df = pd.read_excel(input_file)
    
    # 按关键词分组处理
    for keyword, group in df.groupby('搜索关键词'):
        tweets = group['推文内容'].tolist()
        
        print(f"\n分析关键词: {keyword}")
        print(f"推文数量: {len(tweets)}")
        
        alert_system.analyze_and_alert(tweets, keyword)
        time.sleep(1)

if __name__ == "__main__":
    input_file = os.path.join(os.path.expanduser("~"), "Desktop", "twitter_results.xlsx")
    monitor_and_analyze(input_file) 