import json
import os
from datetime import datetime
import requests
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd
import time
    
class HistoricalMessageAnalyzer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.siliconflow.cn/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # 分析提示词
        self.analysis_prompt = """
请根据以下博主的交易分析内容，提取并整理出以下信息：

1. **交易币种**：提取博主提到的币种名称，最开始的几个字母一般为币种名称。

2. **方向**：提取博主的交易方向（例如：多单、空单）。

3. **杠杆**：如果博主提到杠杆，请标明杠杆倍数。

4. **入场点位**：如果博主提到入场价格，请列出。
   - 可能会有多个入场点位
   - 记录所有提到的可能入场价格

5. **止损点位**：如果博主提到止损价格，请列出。
   - 记录所有提到的止损价位

6. **止盈点位**：如果博主提到止盈价格，请列出。
   - 记录所有提到的止盈价位
   - 通常会有点位1（求稳）和点位2，都需要列出

7. **分析内容**：提取并总结博主针对该币种的市场分析和走势预测。

内容如下：
{content}

请以JSON格式返回分析结果，价格必须是数字（不含单位），未提及的信息用null。
"""

    def _extract_translated_content(self, content: str) -> Tuple[str, str]:
        """提取原文和翻译内容"""
        try:
            if "**原文:**" in content and "**翻译:**" in content:
                parts = content.split("**翻译:**")
                if len(parts) >= 2:
                    original = parts[0].replace("**原文:**", "").strip()
                    translated = parts[1].split("--------------")[0].strip()
                    return original, translated
            return content, content
        except Exception as e:
            print(f"提取翻译内容时出错: {str(e)}")
            return content, content

    def should_analyze_message(self, msg: Dict) -> bool:
        """判断消息是否需要分析"""
        if not msg.get('content'):
            return False
            
        content = msg['content']
        # 跳过太短的消息
        if len(content.strip()) < 10:
            return False
            
        # 检查是否包含价格相关信息
        price_indicators = ['$', '美元', 'k', 'K', '千', '万']
        has_price = any(indicator in content for indicator in price_indicators)
        
        # 检查是否包含交易相关词汇
        trading_keywords = ['多', '空', '做多', '做空', '买入', '卖出', '止损', '止盈', 
                          'long', 'short', 'buy', 'sell', 'stop', 'target']
        has_trading_terms = any(keyword in content.lower() for keyword in trading_keywords)
        
        return has_price or has_trading_terms

    def analyze_message(self, content: str, retry_count: int = 3) -> Optional[Dict]:
        """分析单条消息"""
        original, translated = self._extract_translated_content(content)
        
        # 使用翻译内容进行分析
        content_to_analyze = translated or original
        
        if not content_to_analyze or len(content_to_analyze.strip()) < 10:
            return None
            
        messages = [{"role": "user", "content": self.analysis_prompt.format(content=content_to_analyze)}]
        
        for attempt in range(retry_count):
            try:
                print(f"正在分析消息: {content_to_analyze[:100]}...")
                
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=self.headers,
                    json={
                        "model": "deepseek-ai/DeepSeek-V3",
                        "messages": messages,
                        "max_tokens": 1024,
                        "temperature": 0.7
                    },
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                
                if 'choices' in result and len(result['choices']) > 0:
                    content = result['choices'][0]['message']['content']
                    try:
                        parsed_result = json.loads(content)
                        print("分析成功！")
                        # 添加原文和翻译到结果中
                        parsed_result['原文'] = original
                        parsed_result['翻译'] = translated
                        return parsed_result
                    except json.JSONDecodeError:
                        print(f"JSON解析失败: {content}")
                        return None
                        
            except requests.exceptions.RequestException as e:
                print(f"API请求失败 (尝试 {attempt + 1}/{retry_count}): {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                print(f"未知错误 (尝试 {attempt + 1}/{retry_count}): {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
        
        return None

    def process_message_files(self, data_dir: str, output_dir: str):
        """处理所有消息文件"""
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取所有JSON文件
        json_files = list(Path(data_dir).glob("*.json"))
        total_files = len(json_files)
        
        if not json_files:
            print(f"警告：在目录 {data_dir} 中没有找到JSON文件")
            return
        
        all_results = []
        processed_messages = 0
        skipped_messages = 0
        
        for i, file_path in enumerate(json_files, 1):
            print(f"\n处理文件 {i}/{total_files}: {file_path.name}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                channel_name = self._extract_channel_name(file_path.name)
                print(f"频道名称: {channel_name}")
                
                if isinstance(data, list):  # 如果文件内容直接是消息数组
                    messages = data
                elif isinstance(data, dict) and 'messages' in data:  # 如果消息在messages字段中
                    messages = data['messages']
                else:
                    print(f"警告：文件 {file_path.name} 格式不正确")
                    continue
                    
                print(f"发现 {len(messages)} 条消息")
                
                for j, msg in enumerate(messages, 1):
                    processed_messages += 1
                    
                    if not self.should_analyze_message(msg):
                        skipped_messages += 1
                        print(f"跳过消息 {j}: 不符合分析条件")
                        continue
                    
                    print(f"\n处理消息 {j}/{len(messages)}")
                    result = self.analyze_message(msg.get('content', ''))
                    
                    if result:
                        # 添加元数据
                        enriched_result = {
                            'channel': channel_name,
                            'timestamp': msg.get('timestamp'),
                            'message_id': msg.get('id'),
                            'author': msg.get('author'),
                            'author_id': msg.get('author_id'),
                            'attachments': msg.get('attachments', []),
                            'analysis': result
                        }
                        all_results.append(enriched_result)
                        
                        # 每10条消息保存一次
                        if len(all_results) % 10 == 0:
                            self._save_results(all_results, output_dir)
                    
            except Exception as e:
                print(f"处理文件时出错 {file_path}: {str(e)}")
            
        print(f"\n处理完成:")
        print(f"处理了 {total_files} 个文件")
        print(f"处理了 {processed_messages} 条消息")
        print(f"跳过了 {skipped_messages} 条消息")
        print(f"成功分析了 {len(all_results)} 条消息")
        
        # 最终保存
        if all_results:
            self._save_results(all_results, output_dir)
            self._generate_report(all_results, output_dir)
        else:
            print("警告：没有成功分析任何消息")

    def _extract_channel_name(self, filename: str) -> str:
        """从文件名提取频道名称"""
        parts = filename.split('-')
        if len(parts) >= 2:
            return '-'.join(parts[1:]).replace('.json', '')
        return filename.replace('.json', '')

    def _save_results(self, results: List[Dict], output_dir: str):
        """保存分析结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"analysis_results_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"保存了 {len(results)} 条分析结果到 {output_file}")

    def _generate_report(self, results: List[Dict], output_dir: str):
        """生成分析报告"""
        if not results:
            print("警告：没有分析结果可供生成报告")
            report = {
                "总消息数": 0,
                "频道统计": {},
                "每日消息数": {},
                "币种统计": {},
                "交易方向统计": {}
            }
            
            # 保存空报告
            report_file = os.path.join(output_dir, "analysis_report.json")
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            print(f"已生成空报告：{report_file}")
            return

        # 转换为DataFrame
        df = pd.json_normalize(results)
        
        # 基础统计
        report = {
            "总消息数": len(results),
            "频道统计": df['channel'].value_counts().to_dict() if 'channel' in df.columns else {},
            "每日消息数": df['timestamp'].str[:10].value_counts().to_dict() if 'timestamp' in df.columns else {},
            "币种统计": df['analysis.交易币种'].value_counts().to_dict() if 'analysis.交易币种' in df.columns else {},
            "交易方向统计": df['analysis.方向'].value_counts().to_dict() if 'analysis.方向' in df.columns else {}
        }
        
        # 保存报告
        report_file = os.path.join(output_dir, "analysis_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        # 导出Excel
        excel_file = os.path.join(output_dir, "analysis_data.xlsx")
        df.to_excel(excel_file, index=False)
        
        print(f"\n分析报告已生成：")
        print(f"JSON报告：{report_file}")
        print(f"Excel数据：{excel_file}")

def main():
    # 配置
    api_key = "sk-otojtetvwuxonxxslyscpqksgeqemplmvzlndcpbnkdaexlu"
    data_dir = r"C:\Users\Admin\Desktop\discord-monitor-master0306\data\messages"
    output_dir = "historical_analysis_results"
    
    # 创建分析器实例
    analyzer = HistoricalMessageAnalyzer(api_key)
    
    # 开始处理
    print(f"开始分析历史消息...")
    print(f"数据目录: {data_dir}")
    print(f"输出目录: {output_dir}")
    
    analyzer.process_message_files(data_dir, output_dir)
    
    print("\n分析完成！")

if __name__ == "__main__":
    main() 