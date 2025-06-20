import json
import os
import sys

# 定义关键词列表
keywords = ["Spot", "Long", "Entry", "现货", "做多", "入场价", "止损", "止盈", "Short", "做空", "SL"]

# 默认输入输出文件路径
default_input_file = r'c:\Users\Admin\Desktop\1\颜驰.json'
default_output_file = r'c:\Users\Admin\Desktop\1\颜驰_filtered.json'

def contains_keywords(content):
    """检查内容是否包含任一关键词"""
    if not content:
        return False
    
    for keyword in keywords:
        if keyword in content:
            return True
    return False

def process_json_file(input_file_path, output_file_path):
    """
    处理JSON文件，只保留timestamp、content和inlineEmojis字段
    并删除以下消息：
    1. content以https开头的消息
    2. 包含@deleted-role的消息
    3. content为空的消息
    
    Args:
        input_file_path: 输入JSON文件路径
        output_file_path: 输出JSON文件路径
    """
    try:
        # 读取原始JSON文件
        with open(input_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # 首先打印数据结构，以便调试
        print(f"数据类型: {type(data)}")
        if isinstance(data, list):
            if len(data) > 0:
                print(f"第一个元素类型: {type(data[0])}")
                print(f"第一个元素示例: {data[0]}")
        elif isinstance(data, dict):
            print(f"顶层键: {list(data.keys())}")
            # 检查是否有messages键
            if "messages" in data:
                print("找到messages键")
                data = data["messages"]
        else:
            print(f"未知数据类型: {type(data)}")
        
        # 处理数据，只保留需要的字段
        processed_data = []
        
        # 如果数据是字符串，尝试再次解析
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as e:
                print(f"无法解析数据: {str(e)}")
                return
        
        # 跟踪过滤的消息计数
        filtered_https_count = 0
        filtered_deleted_role_count = 0
        filtered_empty_content_count = 0
        total_messages = 0
        
        # 检查数据是列表还是字典
        if isinstance(data, list):
            total_messages = len(data)
            for message in data:
                if isinstance(message, dict):
                    # 检查content是否以https开头
                    content = message.get("content", "")
                    
                    # 过滤条件1：以https开头的消息
                    if content.strip().lower().startswith("https"):
                        filtered_https_count += 1
                        continue  # 跳过该消息
                    
                    # 过滤条件2：包含@deleted-role的消息
                    if "@deleted-role" in content:
                        filtered_deleted_role_count += 1
                        continue  # 跳过该消息
                    
                    # 过滤条件3：content为空的消息
                    if not content.strip():
                        filtered_empty_content_count += 1
                        continue  # 跳过该消息
                        
                    processed_message = {
                        "timestamp": message.get("timestamp", ""),
                        "content": content,
                        "inlineEmojis": message.get("inlineEmojis", [])
                    }
                    processed_data.append(processed_message)
                else:
                    print(f"跳过非字典元素: {message}")
        elif isinstance(data, dict):
            # 如果是字典，尝试找到messages字段
            if "messages" in data:
                messages = data["messages"]
                if isinstance(messages, list):
                    total_messages = len(messages)
                    for message in messages:
                        if isinstance(message, dict):
                            content = message.get("content", "")
                            
                            # 过滤条件1：以https开头的消息
                            if content.strip().lower().startswith("https"):
                                filtered_https_count += 1
                                continue  # 跳过该消息
                            
                            # 过滤条件2：包含@deleted-role的消息
                            if "@deleted-role" in content:
                                filtered_deleted_role_count += 1
                                continue  # 跳过该消息
                            
                            # 过滤条件3：content为空的消息
                            if not content.strip():
                                filtered_empty_content_count += 1
                                continue  # 跳过该消息
                                
                            processed_message = {
                                "timestamp": message.get("timestamp", ""),
                                "content": content,
                                "inlineEmojis": message.get("inlineEmojis", [])
                            }
                            processed_data.append(processed_message)
                        else:
                            print(f"跳过非字典元素: {message}")
                else:
                    print(f"messages字段不是列表: {type(messages)}")
            else:
                # 如果没有messages字段，直接将字典作为单个消息处理
                total_messages = 1
                content = data.get("content", "")
                
                # 应用过滤条件
                if content.strip().lower().startswith("https"):
                    filtered_https_count += 1
                elif "@deleted-role" in content:
                    filtered_deleted_role_count += 1
                elif not content.strip():
                    filtered_empty_content_count += 1
                else:
                    processed_message = {
                        "timestamp": data.get("timestamp", ""),
                        "content": content,
                        "inlineEmojis": data.get("inlineEmojis", [])
                    }
                    processed_data.append(processed_message)
        else:
            print(f"无法处理的数据类型: {type(data)}")
        
        # 写入新的JSON文件
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(processed_data, file, ensure_ascii=False, indent=2)
        
        print(f"处理完成！共处理了 {total_messages} 条消息")
        print(f"已过滤 {filtered_https_count} 条以https开头的消息")
        print(f"已过滤 {filtered_deleted_role_count} 条包含@deleted-role的消息")
        print(f"已过滤 {filtered_empty_content_count} 条内容为空的消息")
        print(f"最终保留 {len(processed_data)} 条消息")
        print(f"输出文件保存在: {output_file_path}")
        
    except Exception as e:
        print(f"处理过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()

def filter_by_keywords(input_file=default_input_file, output_file=default_output_file):
    """
    根据关键词过滤JSON文件，只保留包含关键词的消息
    只保留content、timestamp和inlineEmojis字段
    
    Args:
        input_file: 输入JSON文件路径
        output_file: 输出JSON文件路径
    """
    try:
        # 检查输入文件是否存在
        if not os.path.exists(input_file):
            print(f"错误: 找不到输入文件 '{input_file}'")
            return False
            
        # 读取JSON文件
        print(f"正在读取文件: {input_file}")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 打印数据类型信息
        print(f"数据类型: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            print(f"第一个元素类型: {type(data[0])}")
            if len(data) > 0:
                print(f"第一个元素示例: {data[0]}")
        elif isinstance(data, dict):
            print(f"顶层键: {list(data.keys())}")
        
        # 统计原始消息数量
        if isinstance(data, list):
            original_count = len(data)
        elif isinstance(data, dict):
            if "messages" in data and isinstance(data["messages"], list):
                data = data["messages"]
                original_count = len(data)
            else:
                original_count = 1
        else:
            original_count = 1
        print(f"原始消息数量: {original_count}")
        
        # 过滤不包含关键词的消息
        filtered_data = []
        removed_count = 0
        
        if isinstance(data, list):
            for item in data:
                # 检查item是字典还是字符串
                if isinstance(item, dict):
                    content = item.get("content", "")
                    # 只包含特定字段的新对象
                    filtered_item = {
                        "timestamp": item.get("timestamp", ""),
                        "content": content,
                        "inlineEmojis": item.get("inlineEmojis", [])
                    }
                elif isinstance(item, str):
                    content = item
                    filtered_item = item  # 字符串类型保持不变
                else:
                    print(f"跳过未知类型元素: {type(item)}")
                    continue
                    
                if contains_keywords(content):
                    filtered_data.append(filtered_item)
                else:
                    removed_count += 1
        elif isinstance(data, dict):
            # 判断是否包含messages字段
            if "messages" in data and isinstance(data["messages"], list):
                messages = data["messages"]
                new_messages = []
                for msg in messages:
                    if isinstance(msg, dict):
                        content = msg.get("content", "")
                        # 只包含特定字段的新对象
                        filtered_msg = {
                            "timestamp": msg.get("timestamp", ""),
                            "content": content,
                            "inlineEmojis": msg.get("inlineEmojis", [])
                        }
                    else:
                        content = str(msg)
                        filtered_msg = content  # 非字典类型保持不变
                        
                    if contains_keywords(content):
                        new_messages.append(filtered_msg)
                    else:
                        removed_count += 1
                
                # 创建新的字典，只包含messages字段
                filtered_data = {"messages": new_messages}
            else:
                # 如果是单个消息
                content = data.get("content", "")
                if contains_keywords(content):
                    filtered_data = {
                        "timestamp": data.get("timestamp", ""),
                        "content": content,
                        "inlineEmojis": data.get("inlineEmojis", [])
                    }
                else:
                    removed_count += 1
                    filtered_data = {}  # 空字典，表示没有匹配项
        elif isinstance(data, str):
            # 如果整个数据就是一个字符串
            if contains_keywords(data):
                filtered_data = data
            else:
                removed_count += 1
                filtered_data = ""  # 空字符串，表示没有匹配项
        else:
            print(f"无法处理的数据类型: {type(data)}")
            return False
        
        # 统计过滤后的消息数量
        if isinstance(filtered_data, list):
            filtered_count = len(filtered_data)
        elif isinstance(filtered_data, dict):
            if "messages" in filtered_data and isinstance(filtered_data["messages"], list):
                filtered_count = len(filtered_data["messages"])
            elif filtered_data:  # 非空字典
                filtered_count = 1
            else:
                filtered_count = 0
        elif filtered_data:  # 非空字符串
            filtered_count = 1
        else:
            filtered_count = 0
            
        print(f"过滤后消息数量: {filtered_count}")
        print(f"已删除不包含关键词的消息: {removed_count} 条")
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 写入过滤后的数据到新文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
        
        print(f"处理完成，已保存到: {output_file}")
        return True
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def print_usage():
    print("用法: python process_messages.py [命令] [输入文件路径] [输出文件路径]")
    print("命令:")
    print("  process: 处理原始JSON文件，删除https开头、@deleted-role和空内容消息")
    print("  filter: 过滤JSON文件，只保留包含指定关键词的消息")
    print("如果不提供参数，将使用默认路径和filter命令：")
    print(f"  默认输入文件: {default_input_file}")
    print(f"  默认输出文件: {default_output_file}")

if __name__ == "__main__":
    # 解析命令行参数
    if len(sys.argv) > 1 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        print_usage()
    elif len(sys.argv) == 1:
        # 使用默认路径和filter命令
        filter_by_keywords()
    elif len(sys.argv) >= 2:
        command = sys.argv[1].lower()
        
        if command == "process":
            # 处理原始JSON
            input_file = default_input_file if len(sys.argv) < 3 else sys.argv[2]
            output_file = default_output_file.replace("_filtered", "_processed") if len(sys.argv) < 4 else sys.argv[3]
            
            # 检查输入文件是否存在
            if not os.path.exists(input_file):
                print(f"错误: 找不到输入文件 '{input_file}'")
            else:
                process_json_file(input_file, output_file)
        
        elif command == "filter":
            # 使用关键词过滤
            input_file = default_input_file if len(sys.argv) < 3 else sys.argv[2]
            output_file = default_output_file if len(sys.argv) < 4 else sys.argv[3]
            filter_by_keywords(input_file, output_file)
        
        else:
            # 未知命令
            print(f"未知命令: {command}")
            print_usage() 