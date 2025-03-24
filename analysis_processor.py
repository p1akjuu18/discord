import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_json_string(json_str: str) -> str:
    """清理JSON字符串中的格式问题"""
    if not isinstance(json_str, str):
        return json_str
        
    # 移除多余的换行和空格
    json_str = re.sub(r'\s*\n\s*', ' ', json_str)
    
    # 修复字段名称格式
    json_str = re.sub(r'"\s*\n*\s*([^"]+)\s*\n*\s*"', r'"\1"', json_str)  # 修复字段名中的换行
    json_str = re.sub(r'\s*"\s*([^"]+)\s*"\s*:\s*', r'"\1": ', json_str)
    
    # 修复数值格式
    json_str = re.sub(r'(\d+\.?\d*)\s*(?=,|]|})', r'\1"', json_str)  # 数字后面直接跟逗号
    json_str = re.sub(r'\s*,\s*(\d+\.?\d*)\s*(?=,|]|})', r', "\1"', json_str)  # 逗号后面跟数字
    json_str = re.sub(r':\s*(\d+\.?\d*)\s*(?=,|]|})', r': "\1"', json_str)  # 冒号后面跟数字
    
    # 修复止损点位和止盈点位的特殊格式
    # 处理多行数字的情况
    numbers = re.findall(r'(\d+\.?\d*)\s*(?=\n|,|]|})', json_str)
    if numbers:
        last_number = numbers[-1]
        if '止损点位' in json_str and not '止损点位3' in json_str:
            json_str = re.sub(rf'{last_number}\s*(?=\n|,|]|}})', f'"{last_number}", "止损点位3": "{last_number}"', json_str)
        elif '止盈点位' in json_str and not '止盈点位3' in json_str:
            json_str = re.sub(rf'{last_number}\s*(?=\n|,|]|}})', f'"{last_number}", "止盈点位3": "{last_number}"', json_str)
    
    # 修复"止盈点，需要进行拆分"的情况
    if '止盈点，需要进行拆分' in json_str:
        # 查找前面提到的数字作为止盈点位
        numbers = re.findall(r'(\d+\.?\d*)', json_str)
        if len(numbers) >= 3:
            replacement = f'"止盈点位1": "{numbers[-3]}", "止盈点位2": "{numbers[-2]}", "止盈点位3": "{numbers[-1]}"'
        else:
            replacement = '"止盈点位1": null, "止盈点位2": null, "止盈点位3": null'
        json_str = re.sub(r'止盈点，需要进行拆分', replacement, json_str)
    
    # 移除多余的逗号和空格
    json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
    json_str = re.sub(r'\s+', ' ', json_str)
    
    # 确保JSON对象和数组的格式正确
    json_str = re.sub(r'}\s*{', '}, {', json_str)
    json_str = re.sub(r']\s*\[', '], [', json_str)
    
    return json_str

class AnalysisProcessor:
    def __init__(self, input_file: str, output_dir: str = "data/analysis_results"):
        """
        初始化分析处理器
        
        参数:
            input_file: 输入JSON文件路径
            output_dir: 输出分析结果目录
        """
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def process_file(self):
        """处理指定的JSON文件"""
        if not self.input_file.exists():
            logger.error(f"文件不存在: {self.input_file}")
            return
            
        logger.info(f"开始处理文件: {self.input_file.name}")
        
        try:
            results = self._process_single_file(self.input_file)
            if results:
                self._save_results(results)
            else:
                logger.warning("没有找到可分析的数据")
        except Exception as e:
            logger.error(f"处理文件时出错: {str(e)}")
    
    def _process_single_file(self, file_path: Path) -> list:
        """处理单个JSON文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析错误: {str(e)}")
                return []
        
        results = []
        processed_count = 0
        valid_count = 0
        
        # 确保数据是列表格式
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            logger.error("文件格式错误: 数据必须是JSON对象或数组")
            return []
        
        total_count = len(data)
        logger.info(f"找到 {total_count} 条消息")
        
        for item in data:
            processed_count += 1
            try:
                if processed_count % 100 == 0:
                    logger.info(f"处理进度: {processed_count}/{total_count}")
                
                processed_results = self._process_message(item)
                if processed_results:
                    if isinstance(processed_results, list):
                        results.extend(processed_results)
                        valid_count += len(processed_results)
                    else:
                        results.append(processed_results)
                        valid_count += 1
            except Exception as e:
                logger.error(f"处理第 {processed_count} 条消息时出错: {str(e)}")
                continue
        
        logger.info(f"处理完成: 总计 {total_count} 条消息，有效 {valid_count} 条")
        return results
    
    def _extract_trading_info(self, content: str) -> list:
        """从内容中提取交易信息"""
        try:
            # 清理和标准化JSON字符串
            cleaned_content = clean_json_string(content)
            
            # 尝试解析JSON
            if cleaned_content.startswith('```json'):
                cleaned_content = cleaned_content.strip('```json').strip('```').strip()
            
            # 如果内容不是以[开头，添加方括号使其成为数组
            if not cleaned_content.strip().startswith('['):
                cleaned_content = f'[{cleaned_content}]'
            
            try:
                data = json.loads(cleaned_content)
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析错误: {str(e)}")
                # 尝试修复常见的JSON格式问题
                cleaned_content = re.sub(r'}\s*{', '}, {', cleaned_content)
                cleaned_content = re.sub(r']\s*\[', '], [', cleaned_content)
                try:
                    data = json.loads(cleaned_content)
                except:
                    return []
            
            # 如果是字典，转换为列表
            if isinstance(data, dict):
                data = [data]
            
            # 确保是列表格式
            if not isinstance(data, list):
                return []
            
            # 处理每个交易信息
            results = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                    
                # 标准化数值字段
                processed_item = {}
                for key, value in item.items():
                    # 处理数值字段
                    if isinstance(value, str):
                        # 尝试转换为数值
                        try:
                            if value.replace('.', '').isdigit():
                                processed_item[key] = float(value)
                            else:
                                processed_item[key] = value
                        except:
                            processed_item[key] = value
                    else:
                        processed_item[key] = value
                    
                # 确保所有必要字段都存在
                required_fields = [
                    '交易币种', '方向', '杠杆',
                    '入场点位1', '入场点位2', '入场点位3',
                    '止损点位1', '止损点位2', '止损点位3',
                    '止盈点位1', '止盈点位2', '止盈点位3',
                    '分析内容'
                ]
                for field in required_fields:
                    if field not in processed_item:
                        processed_item[field] = None
                
                # 确保交易币种统一为大写
                if processed_item['交易币种']:
                    processed_item['交易币种'] = str(processed_item['交易币种']).upper()
                
                results.append(processed_item)
            
            return results
            
        except Exception as e:
            logger.error(f"提取交易信息时出错: {str(e)}")
            return []
    
    def _process_message(self, message: dict) -> list:
        """处理单条消息"""
        if not isinstance(message, dict):
            return None
            
        analysis = message.get('analysis', {})
        if not isinstance(analysis, dict):
            return None
        
        # 构建基础结果
        base_result = {
            'channel': message.get('channel', ''),
            'timestamp': message.get('timestamp', ''),
            'author': message.get('author', {}).get('name', '') if isinstance(message.get('author'), dict) else message.get('author', ''),
            'author_id': message.get('author', {}).get('id', '') if isinstance(message.get('author'), dict) else message.get('author_id', ''),
            'original_message': message.get('original_content', '')
        }
        
        # 检查是否有直接的交易信息
        if any(key in analysis for key in ['交易币种', '方向', '杠杆']):
            result = base_result.copy()
            result.update({
                '交易币种': analysis.get('交易币种'),
                '方向': analysis.get('方向'),
                '杠杆': analysis.get('杠杆'),
                '入场点位1': analysis.get('入场点位1'),
                '入场点位2': analysis.get('入场点位2'),
                '入场点位3': analysis.get('入场点位3'),
                '止损点位1': analysis.get('止损点位1'),
                '止损点位2': analysis.get('止损点位2'),
                '止损点位3': analysis.get('止损点位3'),
                '止盈点位1': analysis.get('止盈点位1'),
                '止盈点位2': analysis.get('止盈点位2'),
                '止盈点位3': analysis.get('止盈点位3'),
                '分析内容': analysis.get('分析内容')
            })
            return [result]
        
        # 检查是否有JSON格式的分析内容
        content = analysis.get('分析内容', '')
        if not content:
            return None
        
        # 提取交易信息
        trading_infos = self._extract_trading_info(content)
        if not trading_infos:
            # 如果没有提取到交易信息，返回原始内容
            result = base_result.copy()
            result.update({'分析内容': content})
            return [result]
        
        # 为每个交易信息添加基础信息
        results = []
        for info in trading_infos:
            result = base_result.copy()
            result.update(info)
            results.append(result)
        
        return results
    
    def _save_results(self, results: list):
        """保存分析结果"""
        if not results:
            logger.warning("没有结果需要保存")
            return
            
        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存为Excel文件
        df = pd.DataFrame(results)
        excel_path = self.output_dir / f"analysis_results_{timestamp}.xlsx"
        df.to_excel(excel_path, index=False)
        logger.info(f"分析结果已保存到: {excel_path}")
        
        # 保存为JSON文件
        json_path = self.output_dir / f"analysis_results_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"分析结果已保存到: {json_path}")

def main():
    # 设置输入文件和输出目录
    input_file = "data/analysis_results/交易员张张子 [1223151231258595489]_results.json"
    output_dir = "data/analysis_results"
    
    # 创建处理器实例
    processor = AnalysisProcessor(input_file, output_dir)
    
    # 处理文件
    processor.process_file()

if __name__ == "__main__":
    main() 