import pandas as pd
import numpy as np
import os
import datetime
from datetime import datetime, timedelta
import re

def load_orders(file_path):
    """加载交易订单数据"""
    df = pd.read_excel(file_path)
    print(f"加载了{len(df)}条订单记录")
    return df

def load_crypto_price_data(crypto_name, date):
    """根据加密货币名称和日期加载对应的价格数据"""
    # 将通用名称映射到文件名前缀
    crypto_map = {
        'BTC': 'btcusdt',
        'ETH': 'ethusdt',
        'SOL': 'solusdt',
        # 可以根据需要添加更多映射
    }
    
    # 如果找不到映射，使用原始名称尝试匹配
    prefix = crypto_map.get(crypto_name.upper(), crypto_name.lower() + 'usdt')
    
    # 获取年月
    year_month = date.strftime('%Y-%m')
    
    # 构建文件路径
    file_path = f'../split_data/{prefix}_history_{year_month}.csv'
    
    if not os.path.exists(file_path):
        print(f"警告: 未找到{file_path}，尝试查找最接近的文件")
        # 如果没有找到对应的月份数据，找到最接近的月份
        data_files = os.listdir('../split_data')
        crypto_files = [f for f in data_files if f.startswith(prefix)]
        
        if not crypto_files:
            print(f"错误: 未找到任何{prefix}相关的数据文件")
            return None
        
        # 找到时间上最接近的文件
        crypto_files.sort()
        closest_file = None
        min_diff = float('inf')
        
        for file in crypto_files:
            match = re.search(r'(\d{4}-\d{2})\.csv$', file)
            if match:
                file_month = match.group(1)
                file_date = datetime.strptime(file_month, '%Y-%m')
                diff = abs((file_date.year - date.year) * 12 + file_date.month - date.month)
                
                if diff < min_diff:
                    min_diff = diff
                    closest_file = file
        
        if closest_file:
            file_path = f'../split_data/{closest_file}'
            print(f"使用最接近的文件: {file_path}")
        else:
            return None
    
    try:
        df = pd.read_csv(file_path)
        # 确保时间列是datetime类型，使用非时区感知的datetime
        df['open_time'] = pd.to_datetime(df['open_time']).dt.tz_localize(None)
        print(f"加载了{len(df)}条{crypto_name}价格记录，时间范围: {df['open_time'].min()} 到 {df['open_time'].max()}")
        return df
    except Exception as e:
        print(f"加载价格数据出错: {e}")
        return None

def find_closest_price(price_df, timestamp):
    """找到最接近给定时间戳的价格记录"""
    if price_df is None or len(price_df) == 0:
        return None
    
    # 确保时间戳是datetime对象，统一使用非时区感知的时间戳
    if isinstance(timestamp, str):
        # 处理不同格式的时间字符串
        try:
            # 对于ISO格式的时间字符串
            if 'T' in timestamp:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"无法解析时间戳: {timestamp}, 错误: {e}")
            return None
    
    # 将时区感知的时间戳转换为非时区感知
    if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
        timestamp = timestamp.replace(tzinfo=None)
    
    # 找到最接近的时间记录
    closest_idx = (price_df['open_time'] - timestamp).abs().idxmin()
    return price_df.iloc[closest_idx]

def process_orders(orders_df):
    """处理订单数据，填充缺失的价格信息"""
    # 初始化price列
    orders_df['price'] = np.nan
    
    # 按照交易币种和时间戳对订单进行排序
    # 使用mixed格式解析不一致的时间格式
    try:
        orders_df['timestamp'] = pd.to_datetime(orders_df['timestamp'], format='mixed')
        # 移除时区信息以确保一致性
        orders_df['timestamp'] = orders_df['timestamp'].dt.tz_localize(None)
    except Exception as e:
        print(f"解析订单时间戳出错: {e}")
        # 尝试逐个解析时间戳
        timestamp_list = []
        for ts in orders_df['timestamp']:
            try:
                dt = pd.to_datetime(ts, format='mixed')
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                timestamp_list.append(dt)
            except:
                print(f"无法解析时间戳: {ts}，使用当前时间替代")
                timestamp_list.append(pd.Timestamp.now().tz_localize(None))
        
        orders_df['timestamp'] = timestamp_list
    
    sorted_orders = orders_df.sort_values(['analysis_交易币种', 'timestamp'])
    
    # 用于存储已处理的订单
    processed_orders = []
    current_crypto = None
    current_order = None
    price_data = None
    
    # 缓存加载过的价格数据
    price_data_cache = {}
    
    # 逐个处理订单
    for idx, order in sorted_orders.iterrows():
        crypto = order['analysis_交易币种']
        
        # 如果这是一个新的交易币种，或者之前没有加载过这个币种的价格数据
        if crypto != current_crypto and not pd.isna(crypto):
            current_crypto = crypto
            # 尝试从缓存中获取价格数据
            if crypto in price_data_cache:
                price_data = price_data_cache[crypto]
            else:
                # 加载该币种的价格数据
                order_date = order['timestamp'].to_pydatetime()
                price_data = load_crypto_price_data(crypto, order_date)
                # 将价格数据缓存
                if price_data is not None:
                    price_data_cache[crypto] = price_data
        
        # 为每个订单查找对应的价格，无论是否有交易币种
        if not pd.isna(crypto) and price_data is not None:
            closest_price_record = find_closest_price(price_data, order['timestamp'])
            if closest_price_record is not None:
                # 添加价格信息
                order_with_price = order.copy()
                order_with_price['price'] = closest_price_record['close']
                processed_orders.append(order_with_price)
                
                # 如果这是第一个带入场点位的订单，记录为当前订单
                has_entry = not pd.isna(order['analysis_入场点位1']) or not pd.isna(order['analysis_入场点位2']) or not pd.isna(order['analysis_入场点位3'])
                if has_entry:
                    current_order = order_with_price.copy()
            else:
                processed_orders.append(order)
        else:
            # 如果没有交易币种或价格数据，直接添加原始订单
            processed_orders.append(order)
            continue
        
        # 检查是否有入场点位，如果没有入场点位但之前有策略单，则可能是实时更新的订单
        has_entry = not pd.isna(order['analysis_入场点位1']) or not pd.isna(order['analysis_入场点位2']) or not pd.isna(order['analysis_入场点位3'])
        
        if not has_entry and current_order is not None and current_crypto == crypto:
            # 这可能是一个实时更新的订单，需要找到对应的价格
            if price_data is not None and 'price' in order and not pd.isna(order['price']):
                current_price = order['price']
                
                # 检查是否是止盈或止损
                entry_price = None
                for entry_col in ['analysis_入场点位1', 'analysis_入场点位2', 'analysis_入场点位3']:
                    if not pd.isna(current_order[entry_col]):
                        try:
                            entry_price = float(current_order[entry_col])
                            break
                        except (ValueError, TypeError):
                            # 如果无法转换为浮点数，尝试下一个入场点位
                            continue
                
                if entry_price is not None:
                    # 根据方向判断止盈止损
                    direction = current_order['analysis_方向']
                    
                    updated_order = processed_orders[-1].copy()  # 使用最后添加的订单（已有价格信息）
                    if direction == 'LONG' or direction == '多':
                        if current_price > entry_price:
                            # 止盈
                            if pd.isna(updated_order['analysis_止盈点位1']):
                                updated_order['analysis_止盈点位1'] = current_price
                            elif pd.isna(updated_order['analysis_止盈点位2']):
                                updated_order['analysis_止盈点位2'] = current_price
                            elif pd.isna(updated_order['analysis_止盈点位3']):
                                updated_order['analysis_止盈点位3'] = current_price
                        else:
                            # 止损
                            if pd.isna(updated_order['analysis_止损点位1']):
                                updated_order['analysis_止损点位1'] = current_price
                            elif pd.isna(updated_order['analysis_止损点位2']):
                                updated_order['analysis_止损点位2'] = current_price
                            elif pd.isna(updated_order['analysis_止损点位3']):
                                updated_order['analysis_止损点位3'] = current_price
                    elif direction == 'SHORT' or direction == '空':
                        if current_price < entry_price:
                            # 止盈
                            if pd.isna(updated_order['analysis_止盈点位1']):
                                updated_order['analysis_止盈点位1'] = current_price
                            elif pd.isna(updated_order['analysis_止盈点位2']):
                                updated_order['analysis_止盈点位2'] = current_price
                            elif pd.isna(updated_order['analysis_止盈点位3']):
                                updated_order['analysis_止盈点位3'] = current_price
                        else:
                            # 止损
                            if pd.isna(updated_order['analysis_止损点位1']):
                                updated_order['analysis_止损点位1'] = current_price
                            elif pd.isna(updated_order['analysis_止损点位2']):
                                updated_order['analysis_止损点位2'] = current_price
                            elif pd.isna(updated_order['analysis_止损点位3']):
                                updated_order['analysis_止损点位3'] = current_price
                    
                    # 添加一个标记，表示这是根据价格数据推断的
                    updated_order['analysis_分析内容'] = f"根据价格数据推断: {'止盈' if (direction in ['LONG', '多'] and current_price > entry_price) or (direction in ['SHORT', '空'] and current_price < entry_price) else '止损'}, 价格: {current_price}"
                    
                    # 替换最后一个添加的订单
                    processed_orders[-1] = updated_order
    
    return pd.DataFrame(processed_orders)

def main():
    # 加载订单数据
    orders_file = '../analysis_results_20250416_213902.xlsx'
    orders_df = load_orders(orders_file)
    
    # 处理订单
    processed_df = process_orders(orders_df)
    
    # 保存处理后的结果
    output_file = '../processed_orders.xlsx'
    processed_df.to_excel(output_file, index=False)
    print(f"处理完成，结果已保存至: {output_file}")

if __name__ == "__main__":
    main() 