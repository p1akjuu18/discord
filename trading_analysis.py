import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# 设置日志
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradingAnalyzer:
    def __init__(self, api_key):
        self.api_key = api_key
        
    def get_analysis_prompt(self, content):
        """构建分析提示词"""
        analysis_prompt = """
请根据以下交易相关内容进行分析，提取并整理每个币种的以下信息：

1. 交易币种：提取提到的币种名称（如BTC/USDT、ETH/USDT等）
2. 交易方向：提取交易方向（多单/空单）
3. 杠杆倍数：如果提到杠杆，标明倍数
4. 入场点位：提取建议的入场价格（可能有多个）
5. 止损点位：提取建议的止损价格（可能有多个）
6. 止盈点位：提取建议的止盈价格（可能有多个）
7. 市场分析：总结对该币种的市场分析观点
8. 技术指标：提取提到的技术指标数据
9. 风险提示：总结潜在风险

注意事项：
- 对于BTC，如果价格是3位数，自动补充为5位数（如915补充为91500）
- 如果某项信息未提供，对应值设为null
- 确保所有数值都是数字格式，不要带单位
- 如果有多个点位，按照由小到大排序
- 每个币种单独生成一个分析结果

请按照以下JSON数组格式返回结果，数组中每个对象代表一个币种的分析：
[
    {{
        "交易币种": "BTC/USDT",
        "交易方向": "多单",
        "杠杆倍数": null,
        "入场点位": [84900, 86510, 87745],
        "止损点位": [83520, 82235, 81268],
        "止盈点位": [89250, null, null],
        "市场分析": "BTC市场分析内容...",
        "技术指标": "BTC技术指标数据...",
        "风险提示": "BTC风险提示..."
    }},
    {{
        "交易币种": "ETH/USDT",
        "交易方向": "多单",
        "杠杆倍数": null,
        "入场点位": [2207, null, null],
        "止损点位": [2172, 2142, 2104],
        "止盈点位": [2235, 2274, 2306],
        "市场分析": "ETH市场分析内容...",
        "技术指标": "ETH技术指标数据...",
        "风险提示": "ETH风险提示..."
    }}
]

分析内容如下：
{content}
"""
        return analysis_prompt

    async def analyze_message(self, message, api_client):
        """分析消息内容"""
        try:
            # 构建提示词
            prompt = self.get_analysis_prompt(message.content)
            
            # 构建消息列表
            messages = [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # 调用API
            logger.info("开始调用API进行分析...")
            response = await api_client.test_completion(
                messages=messages,
                model="deepseek-ai/DeepSeek-V3",
                max_tokens=1024,
                temperature=0.7
            )
            
            if response and 'choices' in response:
                analysis_result = response['choices'][0]['message']['content']
                logger.info("API返回原始结果：")
                logger.info(analysis_result)
                
                # 清理API返回的结果
                cleaned_result = analysis_result
                if cleaned_result.startswith('```json'):
                    cleaned_result = cleaned_result.replace('```json', '', 1)
                if cleaned_result.endswith('```'):
                    cleaned_result = cleaned_result.replace('```', '', 1)
                cleaned_result = cleaned_result.strip()
                
                # 解析JSON结果
                analysis_json = json.loads(cleaned_result)
                
                # 获取当前时间
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 创建空的DataFrame列表
                df_list = []
                
                # 处理每个币种的分析结果
                for coin_data in analysis_json:
                    analysis_data = {
                        '时间': current_time,
                        '频道ID': str(message.channel.id),
                        '原始内容': message.content,
                        '交易币种': coin_data.get('交易币种'),
                        '方向': coin_data.get('交易方向'),
                        '杠杆': coin_data.get('杠杆倍数'),
                        '入场点位1': coin_data.get('入场点位', [None, None, None])[0],
                        '入场点位2': coin_data.get('入场点位', [None, None, None])[1],
                        '入场点位3': coin_data.get('入场点位', [None, None, None])[2],
                        '止损点位1': coin_data.get('止损点位', [None, None, None])[0],
                        '止损点位2': coin_data.get('止损点位', [None, None, None])[1],
                        '止损点位3': coin_data.get('止损点位', [None, None, None])[2],
                        '止盈点位1': coin_data.get('止盈点位', [None, None, None])[0],
                        '止盈点位2': coin_data.get('止盈点位', [None, None, None])[1],
                        '止盈点位3': coin_data.get('止盈点位', [None, None, None])[2],
                        '分析内容': coin_data.get('市场分析')
                    }
                    df_list.append(pd.DataFrame([analysis_data]))
                    
                    # 打印分析结果
                    logger.info(f"\n{coin_data.get('交易币种')}的分析结果：")
                    for key, value in analysis_data.items():
                        logger.info(f"{key}: {value}")
                
                # 合并所有结果
                if df_list:
                    result_df = pd.concat(df_list, ignore_index=True)
                    
                    # 保存到Excel
                    analysis_path = Path('data/trading_analysis.xlsx')
                    analysis_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    try:
                        if analysis_path.exists():
                            existing_df = pd.read_excel(analysis_path)
                            result_df = pd.concat([existing_df, result_df], ignore_index=True)
                        
                        result_df.to_excel(analysis_path, index=False)
                        logger.info(f"\n分析结果已保存到: {analysis_path}")
                        return True
                        
                    except Exception as e:
                        logger.error(f"保存Excel文件时出错: {e}")
                        return False
                else:
                    logger.error("没有找到有效的分析结果")
                    return False
                    
            else:
                logger.error("API返回结果为空")
                return False
                
        except Exception as e:
            logger.error(f"分析消息时发生错误: {str(e)}")
            return False 