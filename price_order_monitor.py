# -*- coding: utf-8 -*-
import json
import time
import pandas as pd
import numpy as np
import os
from datetime import datetime
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
from Binance_price_monitor import BinanceRestPriceMonitor
import threading
import requests
import logging
from pathlib import Path
from Log import log_manager

# 获取日志记录器
logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.WARNING)

# 移除所有现有的处理器
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# 添加处理器
logger.addHandler(logging.StreamHandler())

# 设置所有相关日志记录器的级别为WARNING
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('flask').setLevel(logging.WARNING)
logging.getLogger('socketio').setLevel(logging.WARNING)
logging.getLogger('engineio').setLevel(logging.WARNING)

# 初始化应用
app = Flask(__name__, static_url_path='', static_folder='static')
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', logger=False, engineio_logger=False)

# 创建价格监控器
monitor = BinanceRestPriceMonitor(polling_interval=3)
price_thread = None
start_time = None
last_csv_check_time = 0  # 上次检查CSV文件的时间
csv_check_interval = 5  # 每5秒检查一次CSV文件
last_csv_modification_time = 0  # 上次CSV文件修改时间

# 初始化CSV文件路径
csv_file_path = None  # 先定义为None，在程序启动时初始化

# 全局加权盈亏百分比
weighted_profit = 0
# 监控状态
monitoring_active = False

# 价格数据和订单数据
price_data = {}  # 存储最新价格数据
active_orders = []  # 当前活跃订单
completed_orders = []  # 已完成订单
orders_by_symbol = {}  # 按币种分类的订单

# 支持的交易对
AVAILABLE_SYMBOLS = {
    "BTC": "BTCUSDT", 
    "ETH": "ETHUSDT", 
    "SOL": "SOLUSDT", 
    "XRP": "XRPUSDT",
    "CRV": "CRVUSDT",
    "TIA": "TIAUSDT",
    "BMT": "BMTUSDT"
}  # 添加了更多支持的交易对

# 页面标题配置
TITLE_CONFIG = {
    # 页面标题
    "main_title": "订单与价格实时监控系统",
    # 实时价格
    "realtime_price_title": "实时价格数据",
    # 价格表头
    "price_table_header": {
        "symbol": "交易对",
        "mid_price": "最新价格",
        "bid_price": "买入价",
        "ask_price": "卖出价",
        "change_24h": "24小时变化",
        "update_time": "更新时间"
    },
    # 活跃订单
    "active_orders_title": "活跃订单",
    # 活跃订单表头 - 根据图片中的表格结构更新
    "active_orders_table_header": {
        "channel": "频道",
        "symbol": "交易币种",
        "direction": "方向",
        "publish_time": "发布时间",
        "entry_price": "入场点位1",
        "stop_loss": "止损点位1",
        "target_price": "止盈点位1",
        "profit_pct": "总计加权收益%",
        "result": "结果",
        "hold_time": "均持仓时间",
        "risk_reward_ratio": "风险收益比"
    },
    # 已完成订单
    "completed_orders_title": "已完成订单",
    # 已完成订单表头 - 根据图片中的表格结构更新
    "completed_orders_table_header": {
        "channel": "频道",
        "symbol": "交易币种", 
        "direction": "方向",
        "publish_time": "发布时间",
        "entry_price": "入场点位1",
        "stop_loss": "止损点位1", 
        "target_price": "止盈点位1",
        "profit_pct": "总计加权收益%",
        "result": "结果",
        "hold_time": "均持仓时间",
        "risk_reward_ratio": "风险收益比"
    },
    # 价格图表
    "price_chart_title": "价格历史图表"
}

# 安全地转换时间戳
def safe_convert_timestamp(timestamp):
    if pd.isna(timestamp) or timestamp is pd.NaT:
        return None
    
    if isinstance(timestamp, (pd.Timestamp, datetime)):
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    return str(timestamp)

# 安全地转换浮点数
def safe_convert_float(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# 转换为JSON可序列化格式
def make_json_serializable(obj):
    if obj is pd.NaT or obj is np.nan:
        return None
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    return obj

# 加载订单数据
def load_order_data():
    """加载订单数据"""
    global active_orders, completed_orders, orders_by_symbol
    
    try:
        # 清理数据结构
        processed_orders = []
        active_orders = []
        completed_orders = []
        orders_by_symbol = {}
        
        # 先尝试加载CSV文件中的订单数据
        csv_file_path = os.path.join('data', 'analysis_results', 'all_analysis_results.csv')
        if os.path.exists(csv_file_path):
            try:
                print(f"尝试直接从CSV文件加载订单数据: {csv_file_path}")
                csv_df = pd.read_csv(csv_file_path)
                print(f"CSV文件包含 {len(csv_df)} 行数据")
                
                # 列名
                columns = csv_df.columns.tolist()
                
                # 获取关键列
                entry_col = 'analysis.入场点位1' if 'analysis.入场点位1' in columns else None
                stop_loss_col = 'analysis.止损点位1' if 'analysis.止损点位1' in columns else None
                symbol_col = 'analysis.交易币种' if 'analysis.交易币种' in columns else None
                direction_col = 'analysis.方向' if 'analysis.方向' in columns else None
                
                if entry_col and symbol_col:
                    # 获取有入场价格的行
                    filtered_df = csv_df[csv_df[entry_col].notna()]
                    print(f"找到 {len(filtered_df)} 行有入场价格的数据")
                    
                    if len(filtered_df) > 0:
                        # 只处理有效的币种数据
                        for idx, row in filtered_df.iterrows():
                            try:
                                # 获取交易币种
                                original_symbol = str(row.get(symbol_col, '')).strip()
                                if not original_symbol:
                                    continue
                                
                                # 大写处理
                                symbol_upper = original_symbol.upper()
                                
                                # 支持所有币种类型，包括CSV中出现的币种
                                supported_symbol = None
                                for key in AVAILABLE_SYMBOLS.keys():
                                    if key in symbol_upper:
                                        supported_symbol = key
                                        normalized_symbol = AVAILABLE_SYMBOLS[key]
                                        break
                                
                                # 如果没有匹配的币种，尝试添加自定义币种
                                if not supported_symbol:
                                    # 创建一个简化的币种名称（去除非字母字符）
                                    simple_symbol = ''.join(c for c in symbol_upper if c.isalpha())
                                    if simple_symbol:
                                        supported_symbol = simple_symbol
                                        normalized_symbol = f"{simple_symbol}USDT"
                                    else:
                                        continue
                                
                                # 获取方向
                                direction = '多'  # 默认方向
                                if direction_col and not pd.isna(row.get(direction_col)):
                                    direction_str = str(row.get(direction_col, '')).lower()
                                    if '空' in direction_str or 'short' in direction_str or 'sell' in direction_str:
                                        direction = '空'
                                
                                # 获取入场价格
                                entry_price = row.get(entry_col)
                                if pd.isna(entry_price):
                                    continue
                                    
                                try:
                                    entry_price = float(entry_price)
                                except (ValueError, TypeError):
                                    print(f"行 {idx+1}: 入场价格 '{entry_price}' 无法转换为浮点数，跳过")
                                    continue
                                
                                # 获取止损价格（如果没有，根据方向计算默认值）
                                stop_loss = None
                                if stop_loss_col and not pd.isna(row.get(stop_loss_col)):
                                    try:
                                        stop_loss = float(row.get(stop_loss_col))
                                    except (ValueError, TypeError):
                                        stop_loss = None
                                
                                if stop_loss is None:
                                    # 计算默认止损价格
                                    if direction == '多':
                                        stop_loss = entry_price * 0.95  # 默认止损为入场价格的95%
                                    else:
                                        stop_loss = entry_price * 1.05  # 默认止损为入场价格的105%
                                    print(f"行 {idx+1}: 缺少止损价格，使用默认值: {stop_loss}")
                                
                                # 获取目标价格（如果没有，根据方向和止损计算默认值）
                                target_price = None
                                target_cols = [col for col in columns if '止盈' in col or '目标' in col.lower()]
                                for col in target_cols:
                                    if not pd.isna(row.get(col)):
                                        try:
                                            target_price = float(row.get(col))
                                            break
                                        except (ValueError, TypeError):
                                            pass
                                
                                if target_price is None:
                                    # 计算默认目标价格
                                    if direction == '多':
                                        price_diff = entry_price - stop_loss
                                        target_price = entry_price + price_diff * 2  # 风险收益比2:1
                                    else:
                                        price_diff = stop_loss - entry_price
                                        target_price = entry_price - price_diff * 2  # 风险收益比2:1
                                    print(f"行 {idx+1}: 缺少目标价格，使用默认值: {target_price}")
                                
                                # 获取频道信息
                                channel = 'CSV自动导入'
                                if 'channel' in columns:
                                    channel_val = row.get('channel')
                                    if not pd.isna(channel_val):
                                        channel = str(channel_val)
                                
                                # 获取发布时间
                                publish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                if 'timestamp' in columns:
                                    time_val = row.get('timestamp')
                                    if not pd.isna(time_val):
                                        if isinstance(time_val, str):
                                            publish_time = time_val
                                        elif isinstance(time_val, (pd.Timestamp, datetime)):
                                            publish_time = time_val.strftime('%Y-%m-%d %H:%M:%S')
                                
                                # 创建订单对象
                                order_id = len(processed_orders) + 1
                                risk_reward_ratio = calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss)
                                
                                order = create_order_object(
                                    id_num=order_id,
                                    symbol=original_symbol,
                                    normalized_symbol=normalized_symbol,
                                    direction=direction,
                                    entry_price=entry_price,
                                    average_entry_cost=None,
                                    profit_pct=None,
                                    target_price=target_price,
                                    stop_loss=stop_loss,
                                    exit_price=None,
                                    exit_time=None,
                                    is_completed=False,
                                    channel=channel,
                                    publish_time=publish_time,
                                    risk_reward_ratio=risk_reward_ratio,
                                    hold_time=None,
                                    result="-",
                                    source="all_analysis_results.csv"
                                )
                                
                                # 添加到活跃订单列表
                                active_orders.append(order)
                                processed_orders.append(order)
                                
                                # 添加到按币种分类的字典
                                if supported_symbol not in orders_by_symbol:
                                    orders_by_symbol[supported_symbol] = []
                                orders_by_symbol[supported_symbol].append(order)
                                
                                print(f"直接加载CSV中的订单: {original_symbol} {direction} 入场:{entry_price} 止损:{stop_loss}")
                            
                            except Exception as e:
                                print(f"处理CSV行 {idx+1} 时出错: {e}")
                        
                        print(f"从CSV文件成功加载了 {len(active_orders)} 个活跃订单")
                    else:
                        print("CSV文件中没有有效的入场价格数据")
                else:
                    print(f"CSV文件缺少必要的列：入场点位({entry_col})或交易币种({symbol_col})")
            except Exception as e:
                print(f"从CSV文件加载订单时出错: {e}")
        
        # 从result.xlsx读取数据作为已完成订单
        if os.path.exists('result.xlsx'):
            try:
                df = pd.read_excel('result.xlsx')
                
                # 将所有从result.xlsx加载的订单标记为已完成
                for _, row in df.iterrows():
                    try:
                        # 获取交易币种
                        symbol = row.get('交易币种', None)
                        if pd.isna(symbol) or symbol == "":
                            continue
                        
                        # 严格过滤，只保留BTC、ETH和SOL交易对
                        symbol_upper = str(symbol).upper().replace("USDT", "")
                        
                        if symbol_upper not in AVAILABLE_SYMBOLS:
                            # 跳过不支持的交易对
                            continue  
                        
                        # 标准化交易对名称
                        normalized_symbol = AVAILABLE_SYMBOLS[symbol_upper]
                        
                        # 基本信息
                        direction = row.get('方向', '')  # 多/空
                        entry_price = safe_convert_float(row.get('入场点位1', None))
                        average_entry_cost = safe_convert_float(row.get('averageentrycost', None))
                        publish_time = safe_convert_timestamp(row.get('发布时间', None))
                        
                        # 获取止损和止盈
                        stop_loss = safe_convert_float(row.get('止损点位1', None))
                        target_price = safe_convert_float(row.get('止盈点位1', None))
                        
                        # 收益相关
                        profit_pct = safe_convert_float(row.get('总计加权收益%', None))
                        if pd.isna(profit_pct):
                            profit_pct = safe_convert_float(row.get('总计加权收益pct', None))
                        
                        # 持仓时间
                        avg_hold_time = safe_convert_float(row.get('均持仓时间', None))
                        if pd.isna(avg_hold_time):
                            avg_hold_time = safe_convert_float(row.get('avg持仓时间', None))
                        
                        # 将分钟转换为小时
                        if avg_hold_time is not None:
                            avg_hold_time = avg_hold_time / 60  # 分钟转为小时
                        
                        # 获取频道信息
                        channel = row.get('频道', '-')
                        if pd.isna(channel):
                            channel = '-'
                        
                        # 风险收益比
                        risk_reward_ratio = calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss)
                        
                        # 出场信息
                        exit_price = safe_convert_float(row.get('出场价格', None))
                        exit_time = safe_convert_timestamp(row.get('出场时间', None))
                        
                        # 获取结果信息
                        result = row.get('结果', '-')
                        if pd.isna(result):
                            result = '-'
                        
                        # 创建订单对象，强制标记为已完成
                        order = create_order_object(
                            id_num=len(processed_orders) + 1,
                            symbol=symbol,
                            normalized_symbol=normalized_symbol,
                            direction=direction,
                            entry_price=entry_price,
                            average_entry_cost=average_entry_cost,
                            profit_pct=profit_pct,
                            target_price=target_price,
                            stop_loss=stop_loss,
                            exit_price=exit_price,
                            exit_time=exit_time,
                            is_completed=True,  # 强制标记为已完成
                            channel=channel,
                            publish_time=publish_time,
                            risk_reward_ratio=risk_reward_ratio,
                            hold_time=avg_hold_time,
                            result=result,
                            source="result.xlsx"
                        )
                        
                        processed_orders.append(order)
                        completed_orders.append(order)  # 直接加入已完成订单列表
                        
                        # 按币种分类
                        symbol_key = str(symbol).upper()
                        if symbol_key not in orders_by_symbol:
                            orders_by_symbol[symbol_key] = []
                        orders_by_symbol[symbol_key].append(order)
                        
                    except Exception:
                        pass
            except Exception:
                pass
        
        print(f"订单加载完成: {len(active_orders)} 个活跃订单, {len(completed_orders)} 个已完成订单")
        return True
    except Exception as e:
        print(f"加载订单数据出错: {e}")
        return False

# 计算风险收益比
def calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss):
    """计算风险收益比"""
    if entry_price is not None and target_price is not None and stop_loss is not None:
        if direction == '多':
            # 多单：目标价格应高于入场价，止损应低于入场价
            potential_profit = target_price - entry_price
            potential_loss = entry_price - stop_loss
        else:  # 空单
            # 空单：目标价格应低于入场价，止损应高于入场价
            potential_profit = entry_price - target_price
            potential_loss = stop_loss - entry_price
        
        # 确保计算有效（利润和损失都为正数）
        if potential_profit > 0 and potential_loss > 0:
            return potential_profit / potential_loss
    
    return None  # 无效数据返回None

# 检查订单是否已完成
def check_if_completed(exit_price, exit_time, row):
    """检查订单是否已完成"""
    if exit_price is not None or exit_time is not None:
        return True
    elif '结果' in row and not pd.isna(row['结果']) and str(row['结果']).strip() != "":
        return True
    return False

# 创建订单对象
def create_order_object(id_num, symbol, normalized_symbol, direction, entry_price, average_entry_cost, 
                        profit_pct, target_price, stop_loss, exit_price, exit_time, is_completed, 
                        channel, publish_time, risk_reward_ratio, hold_time, result, source=None,
                        entry_price_2=None, entry_price_3=None, weight_1=0.4, weight_2=0.3, weight_3=0.3):
    """创建标准化的订单对象"""
    return {
        'id': id_num,
        'symbol': symbol,
        'normalized_symbol': normalized_symbol,
        'direction': direction,
        'entry_price': entry_price,
        'entry_price_2': entry_price_2,  # 第二入场点位
        'entry_price_3': entry_price_3,  # 第三入场点位
        'weight_1': weight_1,  # 第一点位权重
        'weight_2': weight_2,  # 第二点位权重
        'weight_3': weight_3,  # 第三点位权重
        'average_entry_cost': average_entry_cost,
        'profit_pct': profit_pct,
        'target_price': target_price,
        'stop_loss': stop_loss,
        'exit_price': exit_price,
        'exit_time': exit_time,
        'current_price': None,
        'current_pnl': None,
        'status': 'completed' if is_completed else 'active',
        'triggered': False,  # 是否已触发入场价
        'triggered_time': None,  # 触发入场价的时间
        'channel': channel,
        'publish_time': publish_time,
        'risk_reward_ratio': risk_reward_ratio,
        'hold_time': hold_time,
        'result': result,
        'source': source,  # 数据来源标记，区分不同数据来源
        'is_weighted': entry_price_2 is not None or entry_price_3 is not None  # 是否使用加权计算
    }

# 更新活跃订单的当前价格和盈亏
def update_order_prices():
    """更新活跃订单的当前价格和盈亏"""
    global active_orders
    
    updated_count = 0
    for order in active_orders:
        try:
            # 跳过已完成的订单
            if order.get('status') == 'completed':
                continue
                
            symbol = order.get('normalized_symbol')
            if not symbol or symbol not in price_data:
                # 找不到价格数据，尝试使用更灵活的匹配
                for price_symbol, price_info in price_data.items():
                    if order.get('symbol', '').upper() in price_symbol:
                        symbol = price_symbol
                        break
                
                # 如果仍然找不到价格数据，跳过
                if not symbol or symbol not in price_data:
                    continue
                
            # 获取当前价格
            current_price = price_data[symbol]['mid']
            
            # 保存当前价格
            order['current_price'] = current_price
            
            # 检查是否已触发入场价
            try:
                entry_price = float(order.get('entry_price', 0))
                if entry_price <= 0:
                    continue
            except (ValueError, TypeError):
                continue
                
            direction = order.get('direction', '多')
            
            # 如果订单未触发，检查是否达到触发条件
            if not order.get('triggered', False):
                is_triggered = False
                
                # 根据方向判断触发条件
                if direction == '多':
                    # 多单：当前价格 <= 入场价时触发
                    if current_price <= entry_price:
                        is_triggered = True
                else:  # 空单
                    # 空单：当前价格 >= 入场价时触发
                    if current_price >= entry_price:
                        is_triggered = True
                
                # 如果触发了，更新订单状态
                if is_triggered:
                    order['triggered'] = True
                    order['triggered_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} {direction}单触发入场：当前价格 {current_price} 已达到入场价 {entry_price}")
            
            # 计算当前盈亏（不管是否触发都计算）
            if current_price > 0:
                # 计算实际或潜在盈亏
                if direction == '多':
                    pnl_pct = (current_price - entry_price) / entry_price * 100
                else:  # 空
                    pnl_pct = (entry_price - current_price) / entry_price * 100
                
                # 所有订单都保存盈亏数据，区分是否已触发
                if order.get('triggered', False):
                    order['profit_pct'] = pnl_pct
                    order['current_pnl'] = pnl_pct
                else:
                    # 未触发的订单保存为潜在盈亏
                    order['potential_pnl'] = pnl_pct
                    # 仍然保存到profit_pct，前端可以根据triggered状态区分显示
                    order['profit_pct'] = pnl_pct
                    order['current_pnl'] = pnl_pct
                
                # 计算盈亏金额
                try:
                    amount = float(order.get('amount', 0))
                    if amount > 0:
                        profit_amount = (amount * pnl_pct) / 100
                        order['profit_amount'] = profit_amount
                except (ValueError, TypeError):
                    pass
                
                # 根据是否触发，显示不同的盈亏信息
                if order.get('triggered', False):
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} {order['direction']} 实时盈亏: {pnl_pct:.2f}% (当前: {current_price} / 入场: {entry_price})")
                else:
                    # 显示为"潜在"盈亏
                    price_diff_pct = abs((entry_price - current_price) / current_price * 100)
                    if direction == '多':
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 多单等待触发: 当前 {current_price} / 目标入场 {entry_price} (差距: {price_diff_pct:.2f}%, 潜在盈亏: {pnl_pct:.2f}%)")
                    else:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 空单等待触发: 当前 {current_price} / 目标入场 {entry_price} (差距: {price_diff_pct:.2f}%, 潜在盈亏: {pnl_pct:.2f}%)")
                
                # 只有已触发的订单才检查止盈止损
                try:
                    stop_loss = float(order.get('stop_loss', 0))
                    target_price = float(order.get('target_price', 0))
                    
                    if order.get('triggered', False) and stop_loss > 0 and target_price > 0 and order.get('result', '-') == '-':
                        # 获取当前时间作为出场时间
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        is_completed = False
                        
                        if order['direction'] == '多':
                            # 多单：如果价格低于止损，标记为止损；如果价格高于目标，标记为止盈
                            if current_price <= stop_loss:
                                order['result'] = '止损'
                                order['exit_price'] = current_price
                                order['exit_time'] = current_time
                                order['status'] = 'completed'
                                is_completed = True
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 多单触发止损: 价格 {current_price} <= 止损价 {stop_loss}")
                            elif current_price >= target_price:
                                order['result'] = '止盈'
                                order['exit_price'] = current_price
                                order['exit_time'] = current_time
                                order['status'] = 'completed'
                                is_completed = True
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 多单触发止盈: 价格 {current_price} >= 目标价 {target_price}")
                        else:  # 空单
                            # 空单：如果价格高于止损，标记为止损；如果价格低于目标，标记为止盈
                            if current_price >= stop_loss:
                                order['result'] = '止损'
                                order['exit_price'] = current_price
                                order['exit_time'] = current_time
                                order['status'] = 'completed'
                                is_completed = True
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 空单触发止损: 价格 {current_price} >= 止损价 {stop_loss}")
                            elif current_price <= target_price:
                                order['result'] = '止盈'
                                order['exit_price'] = current_price
                                order['exit_time'] = current_time
                                order['status'] = 'completed'
                                is_completed = True
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} 空单触发止盈: 价格 {current_price} <= 目标价 {target_price}")
                        
                        # 如果订单完成，计算持仓时间
                        if is_completed and 'triggered_time' in order and order['triggered_time']:
                            try:
                                # 使用触发时间而不是发布时间来计算持仓时间
                                entry_time = datetime.strptime(order['triggered_time'], '%Y-%m-%d %H:%M:%S')
                                exit_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
                                hold_time_seconds = (exit_time - entry_time).total_seconds()
                                hold_time_hours = hold_time_seconds / 3600  # 转换为小时
                                order['hold_time'] = hold_time_hours
                            except Exception:
                                pass
                except (ValueError, TypeError):
                    pass
                
                updated_count += 1
            
            # 如果没有获取到入场价格，但有止损和目标价格，可以进行估算
            elif order['stop_loss'] is not None and order['target_price'] is not None:
                try:
                    stop_loss = float(order.get('stop_loss', 0))
                    target_price = float(order.get('target_price', 0))
                    
                    if stop_loss > 0 and target_price > 0:
                        # 使用止损和目标价格的中间值作为入场价格估算
                        if order['direction'] == '多':
                            estimated_entry = (target_price + stop_loss) / 2
                            pnl_pct = (current_price - estimated_entry) / estimated_entry * 100
                        else:
                            estimated_entry = (target_price + stop_loss) / 2
                            pnl_pct = (estimated_entry - current_price) / estimated_entry * 100
                        
                        order['profit_pct'] = pnl_pct
                        order['current_pnl'] = pnl_pct
                        order['estimated_entry'] = True  # 标记为估算入场价
                        
                        # 计算估算盈亏金额
                        try:
                            amount = float(order.get('amount', 0))
                            if amount > 0:
                                profit_amount = (amount * pnl_pct) / 100
                                order['profit_amount'] = profit_amount
                        except (ValueError, TypeError):
                            pass
                        
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {order['symbol']} {order['direction']} 估算盈亏: {pnl_pct:.2f}% (当前: {current_price} / 估算入场: {estimated_entry:.2f})")
                        updated_count += 1
                except (ValueError, TypeError):
                    pass
        except Exception as e:
            # 记录错误
            print(f"更新订单价格时出错: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    # 返回更新的订单数量
    return updated_count

# 主动更新所有订单的状态
def update_all_orders_status():
    """更新所有订单的状态，检查是否有完成的订单"""
    global active_orders, completed_orders
    
    # 先更新价格和盈亏
    updated_count = update_order_prices()
    
    # 检查是否有订单状态需要更新
    orders_to_move = []
    for i, order in enumerate(active_orders):
        # 检查是否有完成标记
        if (order.get('status') == 'completed' or 
            order.get('result', '-') in ['止盈', '止损'] or 
            order.get('exit_price') is not None or 
            order.get('exit_time') is not None):
            
            # 确保订单有完整的结果信息
            if order.get('result', '-') == '-':
                # 如果没有明确结果但有出场信息，根据盈亏设置结果
                if order.get('profit_pct', 0) > 0:
                    order['result'] = '止盈'
                else:
                    order['result'] = '止损'
            
            # 确保有出场价格
            if order.get('exit_price') is None and order.get('current_price') is not None:
                order['exit_price'] = order['current_price']
            
            # 确保有出场时间
            if order.get('exit_time') is None:
                order['exit_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 状态标记为已完成
            order['status'] = 'completed'
            
            # 添加到待移动列表
            orders_to_move.append(i)
    
    # 从活跃订单列表中移除，并添加到已完成订单列表
    if orders_to_move:
        # 从后往前删除，避免索引变化问题
        for i in sorted(orders_to_move, reverse=True):
            # 获取订单数据，用于输出信息
            order = active_orders[i]
            symbol = order.get('symbol', '未知')
            direction = order.get('direction', '未知')
            result = order.get('result', '未知')
            profit = order.get('profit_pct', 0)
            
            # 打印订单完成信息
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 订单完成: {symbol} {direction} {result} 收益:{profit:.2f}%")
            
            # 移动到已完成列表
            completed_orders.append(order)
            del active_orders[i]
    
    return updated_count

# 接收和发送价格数据的函数
def background_monitoring():
    """
    在后台运行价格和订单监控。
    此函数维护全局价格数据，并定期检查活跃订单的状态更新。
    """
    global active_orders, price_data, monitoring_active
    
    print("开始后台监控线程")
    monitor_start_time = time.time()
    last_status_print_time = time.time()
    pnl_display_interval = 10  # 每10秒打印一次盈亏状态
    
    # 设置监控状态为活跃
    monitoring_active = True
    
    # 创建一个新的会话以避免重复使用现有的
    new_session = requests.Session()
    new_session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    while monitoring_active:
        try:
            loop_start = time.time()
            
            # 更新价格数据
            update_price_data(session=new_session)
            
            # 每2秒检查一次CSV文件更新
            check_csv_updates()
            
            # 每5秒更新一次活跃订单状态
            # 使用分钟整数计数器确保仅在需要时执行
            current_time = time.time()
            if current_time - monitor_start_time >= 5:
                monitor_start_time = current_time
                update_all_orders_status()
                
                # 通过WebSocket发送更新
                if socketio:
                    try:
                        # 发送价格数据更新
                        socketio.emit('price_update', price_data)
                        
                        # 发送活跃订单更新
                        socketio.emit('orders_update', {
                            'active_orders': [format_order_for_display(order) for order in active_orders],
                            'order_count': len(active_orders)
                        })
                    except Exception as e:
                        print(f"WebSocket发送更新时出错: {e}")
            
            # 每10秒打印活跃订单的盈亏状态
            if current_time - last_status_print_time >= pnl_display_interval:
                last_status_print_time = current_time
                print_active_orders_status()
            
            # 控制循环速度，确保每次迭代之间有一点延迟
            loop_duration = time.time() - loop_start
            sleep_time = max(0.1, 1.0 - loop_duration)  # 至少等待0.1秒，最多等待1秒
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"后台监控线程出错: {e}")
            # 错误后短暂暂停，避免CPU过载
            time.sleep(5)
    
    monitoring_active = False
    print("后台监控线程已停止")

def print_active_orders_status():
    """打印所有活跃订单的盈亏状态"""
    if not active_orders:
        return
    
    # 打印表头
    print("\n========== 活跃订单盈亏状态 ==========")
    print(f"{'交易对':<15} {'方向':<4} {'状态':<8} {'盈亏%':<8} {'盈亏额':<10} {'入场价':<10} {'当前价':<10} {'金额':<8}")
    print("-" * 70)
    
    # 按状态和盈亏百分比排序
    sorted_orders = sorted(
        [order for order in active_orders if order.get('status') != 'completed'], 
        key=lambda x: (0 if x.get('triggered', False) else 1, x.get('profit_pct', 0) if x.get('profit_pct') is not None else -9999), 
        reverse=False
    )
    
    # 计算总盈亏金额和总资金
    total_pnl_amount = 0
    total_amount = 0
    
    # 打印每个订单的状态
    for order in sorted_orders:
        symbol = order.get('symbol', 'N/A')
        direction = order.get('direction', 'N/A')
        triggered = order.get('triggered', False)
        status = '已触发' if triggered else '等待中'
        
        # 只计算已触发订单的盈亏
        if triggered:
            profit_pct = order.get('profit_pct', 0)
            profit_amount = order.get('profit_amount', 0)
        else:
            profit_pct = order.get('potential_pnl', 0)
            profit_amount = 0
            
        # 确保entry_price和current_price是数值类型
        try:
            entry_price = float(order.get('entry_price', 0))
            entry_price_display = f"{entry_price:.4f}"
        except (ValueError, TypeError):
            entry_price_display = str(order.get('entry_price', 'N/A'))
            
        try:
            current_price = float(order.get('current_price', 0))
            current_price_display = f"{current_price:.4f}"
        except (ValueError, TypeError):
            current_price_display = str(order.get('current_price', 'N/A'))
            
        try:
            amount = float(order.get('amount', 0))
        except (ValueError, TypeError):
            amount = 0
        
        # 累计总额（只计算已触发的订单）
        if triggered:
            total_pnl_amount += profit_amount if profit_amount else 0
            total_amount += amount if amount else 0
        
        # 根据触发状态和盈亏设置颜色
        if not triggered:
            status_color = '\033[93m'  # 黄色 - 等待中
            profit_str = "   -   "
        else:
            status_color = '\033[96m'  # 青色 - 已触发
            
            if profit_pct > 0:
                profit_color = '\033[92m'  # 绿色
                profit_str = f"{profit_color}{profit_pct:6.2f}%\033[0m"
            elif profit_pct < 0:
                profit_color = '\033[91m'  # 红色
                profit_str = f"{profit_color}{profit_pct:6.2f}%\033[0m"
            else:
                profit_color = '\033[0m'   # 默认色
                profit_str = f"{profit_color}{profit_pct:6.2f}%\033[0m"
        
        # 打印订单信息，处理可能的字符串值
        profit_amount_display = f"{profit_amount:.2f}" if triggered else '-'
        print(f"{symbol:<15} {direction:<4} {status_color}{status:<8}\033[0m {profit_str} {profit_amount_display:<8} {entry_price_display:<10} {current_price_display:<10} {amount:<8.2f}")
    
    # 打印总计（只计算已触发的订单）
    print("-" * 70)
    triggered_count = sum(1 for order in sorted_orders if order.get('triggered', False))
    waiting_count = len(sorted_orders) - triggered_count
    
    total_pnl_pct = (total_pnl_amount / total_amount * 100) if total_amount > 0 else 0
    
    # 设置总盈亏颜色
    if total_pnl_pct > 0:
        total_color = '\033[92m'  # 绿色
    elif total_pnl_pct < 0:
        total_color = '\033[91m'  # 红色
    else:
        total_color = '\033[0m'   # 默认色
        
    print(f"总计: {len(sorted_orders)}个订单 (已触发: {triggered_count}, 等待中: {waiting_count})")
    if triggered_count > 0:
        print(f"已触发订单总盈亏: {total_color}{total_pnl_pct:.2f}%\033[0m | {total_color}{total_pnl_amount:.2f}\033[0m | 总资金: {total_amount:.2f}")
    print("==========================================\n")

@app.route('/')
def index():
    """渲染主页"""
    # 添加控制变量，用于在前端控制是否显示搜索框和刷新按钮
    control_config = {
        'layout_version': 'simple',  # 使用简单布局，完全避免复杂的控件处理
        'show_top_controls': False,  # 不显示顶部控制按钮
        'hide_card_header_controls': False,  # 显示卡片头部的控制按钮
        'single_control_only': True  # 只显示一组控件，完全禁用重复控件
    }
    return render_template('order_price_monitor.html', 
                          symbols=AVAILABLE_SYMBOLS, 
                          title_config=TITLE_CONFIG,
                          control_config=control_config)

@app.route('/health')
def health_check():
    """健康检查端点"""
    return jsonify({"status": "ok", "message": "服务正常运行"})

@app.route('/simple')
def simple_page():
    """提供一个简单的HTML页面，用于测试服务是否正常运行"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>简单测试页面</title>
        <meta charset="utf-8">
    </head>
    <body>
        <h1>服务正常运行！</h1>
        <p>如果您看到此页面，说明Web服务器正常工作。</p>
    </body>
    </html>
    """
    return html

@socketio.on('connect')
def handle_connect():
    """处理WebSocket连接"""
    # 发送初始价格数据
    if price_data:
        socketio.emit('all_prices', {
            'prices': list(price_data.values()),
            'timestamp': time.time()
        })
    
    # 发送初始订单数据（确保可序列化）
    serializable_active_orders = make_json_serializable(active_orders)
    serializable_completed_orders = make_json_serializable(completed_orders)
    
    socketio.emit('orders_update', {
        'active_orders': serializable_active_orders,
        'completed_orders': serializable_completed_orders,
        'timestamp': time.time()
    })
    
    # 发送监控状态
    socketio.emit('monitoring_status', {
        'is_monitoring': monitor.keep_running,
        'start_time': start_time,
        'available_symbols': list(AVAILABLE_SYMBOLS.values()),
        'active_order_count': len(active_orders),
        'completed_order_count': len(completed_orders),
        'title_config': TITLE_CONFIG
    })

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """开始价格监控"""
    global price_thread, start_time, monitoring_active
    if not monitoring_active:
        start_time = time.time()
        price_thread = socketio.start_background_task(background_monitoring)
        socketio.emit('monitoring_status', {
            'is_monitoring': True,
            'start_time': start_time,
            'available_symbols': list(AVAILABLE_SYMBOLS.values()),
            'active_order_count': len(active_orders),
            'completed_order_count': len(completed_orders),
            'title_config': TITLE_CONFIG
        })
        return {'status': 'started'}
    return {'status': 'already_running'}

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """停止价格监控"""
    global monitoring_active
    monitoring_active = False
    socketio.emit('monitoring_status', {
        'is_monitoring': False,
        'title_config': TITLE_CONFIG
    })
    return {'status': 'stopped'}

@socketio.on('refresh_data')
def handle_refresh_data():
    """重新加载订单数据"""
    result = load_order_data()
    if result:
        serializable_active_orders = make_json_serializable(active_orders)
        serializable_completed_orders = make_json_serializable(completed_orders)
        
        socketio.emit('orders_update', {
            'active_orders': serializable_active_orders,
            'completed_orders': serializable_completed_orders,
            'timestamp': time.time()
        })
        
        # 同时发送标题配置
        socketio.emit('title_config_update', {
            'title_config': TITLE_CONFIG
        })
        
        return {'status': 'success', 'message': f'已加载 {len(active_orders)} 个活跃订单，{len(completed_orders)} 个已完成订单'}
    return {'status': 'error', 'message': '加载订单数据失败'}

@socketio.on('set_interval')
def handle_set_interval(data):
    """设置更新间隔"""
    try:
        interval = int(data['interval'])
        if 1 <= interval <= 60:
            monitor.polling_interval = interval
            return {'status': 'success', 'interval': interval}
        return {'status': 'error', 'message': '间隔必须在1-60秒之间'}
    except (KeyError, ValueError) as e:
        return {'status': 'error', 'message': str(e)}

@socketio.on('update_title_config')
def handle_update_title_config(data):
    """更新标题配置"""
    global TITLE_CONFIG
    try:
        if 'title_config' in data and isinstance(data['title_config'], dict):
            # 更新配置
            for key, value in data['title_config'].items():
                if key in TITLE_CONFIG:
                    TITLE_CONFIG[key] = value
            
            # 广播新配置给所有客户端
            socketio.emit('title_config_update', {
                'title_config': TITLE_CONFIG
            })
            
            return {'status': 'success', 'message': '标题配置已更新'}
        return {'status': 'error', 'message': '无效的标题配置数据'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

# 监控CSV文件并更新订单
def monitor_csv_file():
    """监控CSV文件的更新，并加载符合条件的新订单"""
    global active_orders, completed_orders, orders_by_symbol, last_csv_modification_time
    
    try:
        # 检查文件是否存在
        if not os.path.exists(csv_file_path):
            print(f"CSV文件不存在: {csv_file_path}")
            return False
        
        # 获取文件最后修改时间
        current_modification_time = os.path.getmtime(csv_file_path)
        
        # 如果文件没有更新，直接返回
        if current_modification_time <= last_csv_modification_time:
            return False
        
        # 更新最后修改时间
        last_csv_modification_time = current_modification_time
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 检测到CSV文件更新，正在加载新订单...")
        
        # 读取CSV文件
        try:
            csv_df = pd.read_csv(csv_file_path)
            print(f"成功读取CSV文件，共 {len(csv_df)} 行数据")
            
            # 打印列名，用于调试
            columns = csv_df.columns.tolist()
            print(f"CSV文件列名: {columns}")
            
            # 修改：只要有入场点位或止损点位，就将其提取出来
            entry_cols = [col for col in columns if '入场' in col or 'entry' in col.lower()]
            stop_loss_cols = [col for col in columns if '止损' in col or 'stop' in col.lower() or 'sl' in col.lower()]
            
            print(f"找到的入场点位相关列: {entry_cols}")
            print(f"找到的止损点位相关列: {stop_loss_cols}")
            
            # 设置要处理的列
            entry_col = 'analysis.入场点位1' if 'analysis.入场点位1' in columns else (entry_cols[0] if entry_cols else None)
            stop_loss_col = 'analysis.止损点位1' if 'analysis.止损点位1' in columns else (stop_loss_cols[0] if stop_loss_cols else None)
            symbol_col = 'analysis.交易币种' if 'analysis.交易币种' in columns else None
            direction_col = 'analysis.方向' if 'analysis.方向' in columns else None
            
            if not entry_col or not stop_loss_col or not symbol_col:
                print(f"缺少必要的列：入场点位、止损点位或交易币种")
                return False
            
            # 过滤出有有效入场点位的行
            # 修改：放宽限制，只要有入场点位即可，不要求同时有止损点位
            filtered_df = csv_df[csv_df[entry_col].notna()]
            print(f"找到 {len(filtered_df)} 行有入场点位的数据")
            
            if len(filtered_df) == 0:
                print("没有找到有效的入场点位数据")
                return False
            
            # 处理筛选出的数据，创建新订单
            new_orders_count = 0
            processed_ids = set([order.get('id') for order in active_orders + completed_orders])
            
            # 获取当前活跃订单的最大ID
            max_id = max([order.get('id', 0) for order in active_orders + completed_orders], default=2000)
            
            # 处理每一行有效数据
            for idx, row in filtered_df.iterrows():
                try:
                    # 获取或生成订单ID
                    order_id = max_id + 1 + new_orders_count
                    
                    # 获取交易币种
                    original_symbol = row.get(symbol_col)
                    if pd.isna(original_symbol) or original_symbol == "":
                        continue
                    
                    # 修改：支持所有币种，不再限制BTC/ETH/SOL
                    symbol_upper = str(original_symbol).upper()
                    
                    # 支持所有币种，使用原始币种作为标准化币种
                    # 如果是BTC/ETH/SOL，使用标准格式，否则使用原始名称
                    if 'BTC' in symbol_upper:
                        normalized_symbol = 'BTCUSDT'
                        supported_symbol = 'BTC'
                    elif 'ETH' in symbol_upper:
                        normalized_symbol = 'ETHUSDT'
                        supported_symbol = 'ETH'
                    elif 'SOL' in symbol_upper:
                        normalized_symbol = 'SOLUSDT'
                        supported_symbol = 'SOL'
                    else:
                        # 修改：支持其他币种
                        normalized_symbol = f"{symbol_upper}USDT"
                        supported_symbol = symbol_upper
                    
                    # 获取方向
                    direction = '多'  # 默认方向
                    if direction_col and not pd.isna(row.get(direction_col)):
                        direction_str = str(row.get(direction_col)).lower()
                        if '空' in direction_str or 'short' in direction_str or 'sell' in direction_str:
                            direction = '空'
                    
                    # 获取入场价格
                    entry_price = row.get(entry_col)
                    if pd.isna(entry_price):
                        continue
                    
                    try:
                        entry_price = float(entry_price)
                    except (ValueError, TypeError):
                        print(f"行 {idx+1}: 入场价格 '{entry_price}' 无法转换为浮点数，跳过")
                        continue
                    
                    # 获取止损价格（如果没有，则计算一个默认值）
                    stop_loss = row.get(stop_loss_col) if not pd.isna(row.get(stop_loss_col)) else None
                    
                    try:
                        if stop_loss is not None:
                            stop_loss = float(stop_loss)
                        else:
                            # 如果没有止损价格，根据方向计算一个默认值
                            if direction == '多':
                                stop_loss = entry_price * 0.95  # 默认止损为入场价格的95%
                            else:
                                stop_loss = entry_price * 1.05  # 默认止损为入场价格的105%
                    except (ValueError, TypeError):
                        print(f"行 {idx+1}: 止损价格 '{stop_loss}' 无法转换为浮点数，使用默认值")
                        if direction == '多':
                            stop_loss = entry_price * 0.95
                        else:
                            stop_loss = entry_price * 1.05
                    
                    # 获取目标价格（如果没有，则计算一个默认值）
                    target_price = None
                    for col in columns:
                        if '止盈' in col or '目标' in col.lower() or 'target' in col.lower() or 'tp' in col.lower():
                            if not pd.isna(row.get(col)):
                                try:
                                    target_price = float(row.get(col))
                                    break
                                except (ValueError, TypeError):
                                    pass
                    
                    if target_price is None:
                        # 如果没有目标价格，根据方向和止损计算一个默认值
                        if direction == '多':
                            price_diff = entry_price - stop_loss
                            target_price = entry_price + price_diff * 2  # 风险收益比2:1
                        else:
                            price_diff = stop_loss - entry_price
                            target_price = entry_price - price_diff * 2  # 风险收益比2:1
                    
                    # 获取频道或来源信息
                    channel = 'CSV自动导入'
                    for col in columns:
                        if 'channel' in col.lower() or '频道' in col or '来源' in col:
                            channel_val = row.get(col)
                            if not pd.isna(channel_val):
                                channel = str(channel_val)
                                break
                    
                    # 获取发布时间
                    publish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    for col in columns:
                        if 'time' in col.lower() or '时间' in col or 'date' in col:
                            time_val = row.get(col)
                            if not pd.isna(time_val):
                                if isinstance(time_val, str):
                                    publish_time = time_val
                                elif isinstance(time_val, (pd.Timestamp, datetime)):
                                    publish_time = time_val.strftime('%Y-%m-%d %H:%M:%S')
                                break
                    
                    # 计算风险收益比
                    risk_reward_ratio = calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss)
                    
                    # 创建订单对象
                    new_order = create_order_object(
                        id_num=order_id,
                        symbol=original_symbol,
                        normalized_symbol=normalized_symbol,
                        direction=direction,
                        entry_price=entry_price,
                        average_entry_cost=None,
                        profit_pct=None,
                        target_price=target_price,
                        stop_loss=stop_loss,
                        exit_price=None,
                        exit_time=None,
                        is_completed=False,
                        channel=channel,
                        publish_time=publish_time,
                        risk_reward_ratio=risk_reward_ratio,
                        hold_time=None,
                        result="-",
                        source="all_analysis_results.csv"
                    )
                    
                    # 添加到活跃订单列表
                    active_orders.append(new_order)
                    new_orders_count += 1
                    
                    # 按币种分类
                    if supported_symbol not in orders_by_symbol:
                        orders_by_symbol[supported_symbol] = []
                    orders_by_symbol[supported_symbol].append(new_order)
                    
                    print(f"添加新订单: {original_symbol} {direction} 入场:{entry_price} 止损:{stop_loss}")
                    
                except Exception as e:
                    print(f"处理CSV行 {idx+1} 时出错: {e}")
            
            if new_orders_count > 0:
                print(f"成功添加 {new_orders_count} 个新订单")
                return True
            else:
                print("没有添加新订单")
                return False
            
        except Exception as e:
            print(f"读取CSV文件时出错: {e}")
            return False
        
    except Exception as e:
        print(f"监控CSV文件时出错: {e}")
        return False

@socketio.on('refresh_csv')
def handle_refresh_csv():
    """手动刷新CSV文件"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 收到手动刷新CSV文件请求")
    
    # 尝试刷新CSV文件
    result = monitor_csv_file()
    
    if result:
        # 更新成功，发送新的订单数据
        serializable_active_orders = make_json_serializable(active_orders)
        serializable_completed_orders = make_json_serializable(completed_orders)
        
        socketio.emit('orders_update', {
            'active_orders': serializable_active_orders,
            'completed_orders': serializable_completed_orders,
            'timestamp': time.time()
        })
        
        return {'status': 'success', 'message': f'CSV文件刷新成功，当前活跃订单: {len(active_orders)}个'}
    else:
        return {'status': 'info', 'message': 'CSV文件无更新或未找到符合条件的数据'}

# 添加密码验证函数
def verify_admin_password(password):
    """验证管理员密码"""
    if not password or password != ADMIN_PASSWORD:
        logger.warning("密码验证失败")
        return False
    return True

@socketio.on('edit_order')
def handle_edit_order(data):
    """处理编辑订单请求"""
    global active_orders, orders_by_symbol
    
    try:
        # 验证密码
        if not verify_admin_password(data.get('admin_password')):
            logger.warning("编辑订单失败：密码验证失败")
            result = {'status': 'error', 'message': '无权限，密码错误'}
            emit('edit_order_response', result)
            return result
            
        if 'order_id' not in data or 'updated_data' not in data:
            logger.warning("编辑订单失败：缺少必要参数")
            result = {'status': 'error', 'message': '缺少必要参数: order_id 或 updated_data'}
            emit('edit_order_response', result)
            return result
        
        order_id = int(data['order_id'])
        updated_data = data['updated_data']
        
        # 查找对应ID的订单
        order_index = -1
        for i, order in enumerate(active_orders):
            if order.get('id') == order_id:
                order_index = i
                break
        
        if order_index == -1:
            logger.warning(f"编辑订单失败：未找到ID为{order_id}的订单")
            result = {'status': 'error', 'message': f'未找到ID为{order_id}的订单'}
            emit('edit_order_response', result)
            return result
        
        # 保存原始订单数据用于恢复
        original_order = dict(active_orders[order_index])
        logger.info(f"开始编辑订单 ID: {order_id}, 原始数据: {original_order}")
        
        # 验证价格逻辑
        try:
            entry_price = float(updated_data.get('entry_price', original_order['entry_price']))
            stop_loss = float(updated_data.get('stop_loss', original_order['stop_loss']))
            target_price = float(updated_data.get('target_price', original_order['target_price']))
            direction = updated_data.get('direction', original_order['direction'])
            
            # 验证价格逻辑
            if direction == '多':
                if stop_loss >= entry_price:
                    logger.warning(f"编辑订单失败：多单止损价格 {stop_loss} 必须低于入场价格 {entry_price}")
                    result = {'status': 'error', 'message': '多单的止损价格必须低于入场价格'}
                    emit('edit_order_response', result)
                    return result
                if target_price <= entry_price:
                    logger.warning(f"编辑订单失败：多单目标价格 {target_price} 必须高于入场价格 {entry_price}")
                    result = {'status': 'error', 'message': '多单的目标价格必须高于入场价格'}
                    emit('edit_order_response', result)
                    return result
            else:  # 空单
                if stop_loss <= entry_price:
                    logger.warning(f"编辑订单失败：空单止损价格 {stop_loss} 必须高于入场价格 {entry_price}")
                    result = {'status': 'error', 'message': '空单的止损价格必须高于入场价格'}
                    emit('edit_order_response', result)
                    return result
                if target_price >= entry_price:
                    logger.warning(f"编辑订单失败：空单目标价格 {target_price} 必须低于入场价格 {entry_price}")
                    result = {'status': 'error', 'message': '空单的目标价格必须低于入场价格'}
                    emit('edit_order_response', result)
                    return result
        except (ValueError, TypeError) as e:
            logger.error(f"编辑订单失败：价格数据格式错误 - {str(e)}")
            result = {'status': 'error', 'message': f'价格数据格式错误: {str(e)}'}
            emit('edit_order_response', result)
            return result
        
        # 更新可编辑字段
        allowed_fields = [
            'symbol', 'direction', 'entry_price', 'target_price', 'stop_loss', 
            'channel', 'publish_time', 'result'
        ]
        
        # 创建更新后的订单数据副本
        updated_order = dict(original_order)
        
        for field in allowed_fields:
            if field in updated_data:
                if field in ['entry_price', 'target_price', 'stop_loss']:
                    try:
                        updated_order[field] = float(updated_data[field])
                    except (ValueError, TypeError):
                        logger.error(f"编辑订单失败：字段 {field} 的值必须是有效的数字")
                        result = {'status': 'error', 'message': f'字段 {field} 的值必须是有效的数字'}
                        emit('edit_order_response', result)
                        return result
                else:
                    updated_order[field] = updated_data[field]
        
        # 如果交易币种发生变化，更新normalized_symbol
        if 'symbol' in updated_data:
            symbol_upper = str(updated_data['symbol']).upper()
            # 检查是否为已知交易对
            for key, value in AVAILABLE_SYMBOLS.items():
                if key in symbol_upper:
                    updated_order['normalized_symbol'] = value
                    break
            else:
                # 如果没有找到匹配的交易对，使用简单处理
                simple_symbol = ''.join(c for c in symbol_upper if c.isalpha())
                if simple_symbol:
                    updated_order['normalized_symbol'] = f"{simple_symbol}USDT"
                else:
                    logger.error(f"编辑订单失败：无效的交易币种 {symbol_upper}")
                    result = {'status': 'error', 'message': '无效的交易币种'}
                    emit('edit_order_response', result)
                    return result
        
        # 如果更新了入场价、目标价或止损价，重新计算风险收益比
        if any(field in updated_data for field in ['entry_price', 'target_price', 'stop_loss']):
            direction = updated_order['direction']
            entry_price = updated_order['entry_price']
            target_price = updated_order['target_price']
            stop_loss = updated_order['stop_loss']
            
            risk_reward_ratio = calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss)
            updated_order['risk_reward_ratio'] = risk_reward_ratio
        
        # 更新CSV文件
        try:
            if os.path.exists(csv_file_path):
                df = pd.read_csv(csv_file_path)
                mask = None
                if 'id' in df.columns:
                    mask = df['id'] == order_id
                    if not mask.any():
                        logger.warning(f"在CSV文件中未找到要更新的订单行: ID={order_id}")
                        # 尝试使用更多字段组合来确保找到正确的行
                        if all(col in df.columns for col in ['analysis.交易币种', 'analysis.入场点位1', 'analysis.方向', 'channel']):
                            mask = (
                                (df['analysis.交易币种'] == original_order['symbol']) & 
                                (df['analysis.入场点位1'] == original_order['entry_price']) &
                                (df['analysis.方向'] == original_order['direction']) &
                                (df['channel'] == original_order['channel'])
                            )
                            if not mask.any():
                                logger.error(f"使用严格条件仍未找到要更新的订单行: ID={order_id}")
                                # 最后尝试使用最宽松的条件
                                if all(col in df.columns for col in ['analysis.交易币种', 'analysis.方向']):
                                    mask = (
                                        (df['analysis.交易币种'] == original_order['symbol']) & 
                                        (df['analysis.方向'] == original_order['direction'])
                                    )
                                    if not mask.any():
                                        logger.error(f"使用宽松条件仍未找到要更新的订单行: ID={order_id}")
                                        result = {'status': 'error', 'message': '在CSV文件中未找到要更新的订单'}
                                        emit('edit_order_response', result)
                                        return result
                                else:
                                    result = {'status': 'error', 'message': 'CSV文件缺少必要的列，无法定位订单'}
                                    emit('edit_order_response', result)
                                    return result
                        else:
                            result = {'status': 'error', 'message': 'CSV文件缺少必要的列，无法定位订单'}
                            emit('edit_order_response', result)
                            return result
                else:
                    # 如果没有id列，使用更多字段组合来确保找到正确的行
                    if all(col in df.columns for col in ['analysis.交易币种', 'analysis.入场点位1', 'analysis.方向', 'channel']):
                        mask = (
                            (df['analysis.交易币种'] == original_order['symbol']) & 
                            (df['analysis.入场点位1'] == original_order['entry_price']) &
                            (df['analysis.方向'] == original_order['direction']) &
                            (df['channel'] == original_order['channel'])
                        )
                        if not mask.any():
                            logger.error(f"使用严格条件仍未找到要更新的订单行: 没有id列")
                            # 最后尝试使用最宽松的条件
                            if all(col in df.columns for col in ['analysis.交易币种', 'analysis.方向']):
                                mask = (
                                    (df['analysis.交易币种'] == original_order['symbol']) & 
                                    (df['analysis.方向'] == original_order['direction'])
                                )
                                if not mask.any():
                                    logger.error(f"使用宽松条件仍未找到要更新的订单行: 没有id列")
                                    result = {'status': 'error', 'message': '在CSV文件中未找到要更新的订单'}
                                    emit('edit_order_response', result)
                                    return result
                            else:
                                result = {'status': 'error', 'message': 'CSV文件缺少必要的列，无法定位订单'}
                                emit('edit_order_response', result)
                                return result
                    else:
                        result = {'status': 'error', 'message': 'CSV文件缺少必要的列，无法定位订单'}
                        emit('edit_order_response', result)
                        return result
                # 记录找到的行数
                matching_rows = mask.sum() if mask is not None else 0
                if matching_rows == 0:
                    # 自动模糊匹配：币种+方向+入场价误差在1e-6以内
                    import numpy as np
                    candidates = df
                    if 'analysis.交易币种' in df.columns:
                        candidates = candidates[candidates['analysis.交易币种'] == original_order['symbol']]
                    if 'analysis.方向' in df.columns:
                        candidates = candidates[candidates['analysis.方向'] == original_order['direction']]
                    if 'analysis.入场点位1' in df.columns:
                        try:
                            candidates = candidates[np.isclose(candidates['analysis.入场点位1'].astype(float), float(original_order['entry_price']), atol=1e-6)]
                        except Exception as e:
                            logger.error(f"模糊匹配入场价时出错: {e}")
                    if len(candidates) > 0:
                        mask = df.index.isin(candidates.index)
                        logger.warning(f"未找到完全匹配，已自动采用模糊匹配行 index={list(candidates.index)} 进行修正")
                        matching_rows = mask.sum()
                    else:
                        result = {'status': 'error', 'message': '在CSV文件中未找到要更新的订单（已尝试模糊匹配）'}
                        emit('edit_order_response', result)
                        return result
                if matching_rows > 1:
                    logger.warning(f"在CSV文件中找到多个匹配的行 ({matching_rows}行)，将更新所有匹配的行")
                # 更新找到的行
                for field in allowed_fields:
                    if field in updated_data:
                        csv_field = f'analysis.{field}' if field in ['symbol', 'direction', 'entry_price', 'stop_loss', 'target_price'] else field
                        if csv_field in df.columns:
                            df.loc[mask, csv_field] = updated_data[field]
                # 保存更新后的CSV
                backup_path = f"{csv_file_path}.bak"
                try:
                    # 先创建备份
                    df.to_csv(backup_path, index=False)
                    # 如果备份成功，再更新原文件
                    df.to_csv(csv_file_path, index=False)
                    logger.info(f"成功更新CSV文件中的订单 ID: {order_id}")
                except Exception as e:
                    logger.error(f"保存CSV文件时出错: {str(e)}")
                    # 如果更新失败，尝试恢复备份
                    if os.path.exists(backup_path):
                        os.replace(backup_path, csv_file_path)
                    raise
                finally:
                    # 清理备份文件
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
        except Exception as e:
            logger.error(f"更新CSV文件时出错: {str(e)}")
            # 回滚内存中的更改
            active_orders[order_index] = original_order
            result = {'status': 'error', 'message': f'更新CSV文件失败: {str(e)}'}
            emit('edit_order_response', result)
            return result
        
        # 更新内存中的订单数据
        active_orders[order_index] = updated_order
        
        # 更新按币种分类的订单字典
        for symbol, orders in orders_by_symbol.items():
            for i, order in enumerate(orders):
                if order.get('id') == order_id:
                    orders_by_symbol[symbol][i] = updated_order
                    break
        
        # 将更新后的订单数据发送给所有客户端
        serializable_active_orders = make_json_serializable(active_orders)
        socketio.emit('orders_update', {
            'active_orders': serializable_active_orders,
            'timestamp': time.time()
        })
        
        logger.info(f"订单已成功编辑: ID={order_id}")
        result = {'status': 'success', 'message': '订单更新成功'}
        emit('edit_order_response', result)
        return result
    
    except Exception as e:
        logger.error(f"编辑订单时出错: {str(e)}")
        # 如果发生异常，尝试回滚内存中的更改
        if 'order_index' in locals() and order_index != -1 and 'original_order' in locals():
            active_orders[order_index] = original_order
        result = {'status': 'error', 'message': f'编辑订单失败: {str(e)}'}
        emit('edit_order_response', result)
        return result

@socketio.on('delete_order')
def handle_delete_order(data):
    global active_orders, orders_by_symbol
    
    try:
        # 验证密码
        if not verify_admin_password(data.get('admin_password')):
            return {'status': 'error', 'message': '无权限，密码错误'}
        
        # 获取订单ID
        order_id = int(data['order_id'])
        logger.info(f"尝试删除订单 ID: {order_id}")
        
        # 找到要删除的订单
        order_to_delete = None
        for order in active_orders:
            if order.get('id') == order_id:
                order_to_delete = order
                break
                
        if not order_to_delete:
            logger.warning(f"删除订单失败：未找到ID为{order_id}的订单")
            return {'status': 'error', 'message': f'未找到ID为{order_id}的订单'}
        
        # 从CSV文件中删除
        try:
            if os.path.exists(csv_file_path):
                logger.info(f"正在从CSV文件删除订单: {csv_file_path}")
                
                # 读取CSV文件
                df = pd.read_csv(csv_file_path)
                original_rows = len(df)
                
                # 尝试通过ID删除
                if 'id' in df.columns:
                    df = df[df['id'] != order_id]
                else:
                    # 如果没有ID列，使用symbol和entry_price组合删除
                    symbol_col = 'analysis.交易币种'
                    entry_col = 'analysis.入场点位1'
                    
                    if symbol_col in df.columns and entry_col in df.columns:
                        df = df[~((df[symbol_col] == order_to_delete['symbol']) & 
                                (df[entry_col] == order_to_delete['entry_price']))]
                    else:
                        logger.error("CSV文件缺少必要的列用于删除订单")
                        return {'status': 'error', 'message': 'CSV文件格式不正确，无法删除订单'}
                
                # 检查是否成功删除
                if len(df) < original_rows:
                    # 使用安全的保存方法
                    if save_to_csv(df):
                        logger.info(f"成功从CSV文件删除订单，行数从 {original_rows} 减少到 {len(df)}")
                    else:
                        logger.error("保存CSV文件失败")
                        return {'status': 'error', 'message': '保存CSV文件失败'}
                else:
                    logger.warning("CSV文件中未找到匹配的订单记录")
        except Exception as e:
            logger.error(f"从CSV文件删除订单时出错: {str(e)}")
            return {'status': 'error', 'message': f'从CSV文件删除订单失败: {str(e)}'}
        
        # 从内存中删除
        try:
            # 从活跃订单列表中删除
            active_orders = [o for o in active_orders if o.get('id') != order_id]
            
            # 从按币种分类的字典中删除
            for symbol, orders in orders_by_symbol.items():
                orders_by_symbol[symbol] = [o for o in orders if o.get('id') != order_id]
            
            logger.info(f"成功从内存中删除订单 ID: {order_id}")
        except Exception as e:
            logger.error(f"从内存中删除订单时出错: {str(e)}")
            return {'status': 'error', 'message': f'从内存中删除订单失败: {str(e)}'}
        
        # 通知前端更新
        try:
            serializable_active_orders = make_json_serializable(active_orders)
            socketio.emit('orders_update', {
                'active_orders': serializable_active_orders,
                'timestamp': time.time()
            })
            logger.info("已通知前端更新订单列表")
        except Exception as e:
            logger.error(f"通知前端更新时出错: {str(e)}")
            # 继续执行，因为订单已经成功删除
        
        return {'status': 'success', 'message': '订单已成功删除'}
        
    except Exception as e:
        logger.error(f"删除订单过程中发生错误: {str(e)}")
        return {'status': 'error', 'message': f'删除订单失败: {str(e)}'}

@socketio.on('add_order')
def handle_add_order(data):
    global active_orders, orders_by_symbol
    
    try:
        # 验证密码
        if not verify_admin_password(data.get('admin_password')):
            return {'status': 'error', 'message': '无权限，密码错误'}
            
        # 验证必要字段
        required_fields = ['symbol', 'direction', 'entry_price']
        for field in required_fields:
            if field not in data:
                return {'status': 'error', 'message': f'缺少必要字段: {field}'}
        
        # 生成新订单ID
        max_id = max([order.get('id', 0) for order in active_orders + completed_orders], default=0)
        
        # 处理输入数据
        symbol = data.get('symbol')
        direction = data.get('direction', '多')
        
        try:
            entry_price = float(data.get('entry_price'))
        except (ValueError, TypeError):
            return {'status': 'error', 'message': '入场价格必须是有效的数字'}
        
        # 处理止损价格
        try:
            stop_loss = float(data.get('stop_loss', 0))
            if stop_loss <= 0:
                if direction == '多':
                    stop_loss = entry_price * 0.95
                else:
                    stop_loss = entry_price * 1.05
        except (ValueError, TypeError):
            if direction == '多':
                stop_loss = entry_price * 0.95
            else:
                stop_loss = entry_price * 1.05
        
        # 处理目标价格
        try:
            target_price = float(data.get('target_price', 0))
            if target_price <= 0:
                if direction == '多':
                    price_diff = entry_price - stop_loss
                    target_price = entry_price + price_diff * 2
                else:
                    price_diff = stop_loss - entry_price
                    target_price = entry_price - price_diff * 2
        except (ValueError, TypeError):
            if direction == '多':
                price_diff = entry_price - stop_loss
                target_price = entry_price + price_diff * 2
            else:
                price_diff = stop_loss - entry_price
                target_price = entry_price - price_diff * 2
        
        # 处理交易对
        symbol_upper = str(symbol).upper()
        normalized_symbol = None
        for key, value in AVAILABLE_SYMBOLS.items():
            if key in symbol_upper:
                normalized_symbol = value
                break
        
        if not normalized_symbol:
            simple_symbol = ''.join(c for c in symbol_upper if c.isalpha())
            if simple_symbol:
                normalized_symbol = f"{simple_symbol}USDT"
            else:
                return {'status': 'error', 'message': '无效的交易币种'}
        
        # 创建新订单对象
        channel = data.get('channel', '手动添加')
        publish_time = data.get('publish_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        risk_reward_ratio = calculate_risk_reward_ratio(direction, entry_price, target_price, stop_loss)
        
        new_order = create_order_object(
            id_num=max_id + 1,
            symbol=symbol,
            normalized_symbol=normalized_symbol,
            direction=direction,
            entry_price=entry_price,
            average_entry_cost=None,
            profit_pct=None,
            target_price=target_price,
            stop_loss=stop_loss,
            exit_price=None,
            exit_time=None,
            is_completed=False,
            channel=channel,
            publish_time=publish_time,
            risk_reward_ratio=risk_reward_ratio,
            hold_time=None,
            result="-",
            source="manual_add"
        )
        
        # 写入CSV文件
        try:
            if os.path.exists(csv_file_path):
                df = pd.read_csv(csv_file_path)
                # 准备新行数据
                new_row = {
                    'analysis.交易币种': symbol,
                    'analysis.方向': direction,
                    'analysis.入场点位1': entry_price,
                    'analysis.止损点位1': stop_loss,
                    'analysis.止盈点位1': target_price,
                    'channel': channel,
                    'timestamp': publish_time
                }
                # 添加新行
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                if save_to_csv(df):
                    logger.info("成功将新订单写入CSV文件")
                else:
                    logger.error("保存CSV文件失败")
                    return {'status': 'error', 'message': '保存CSV文件失败'}
            else:
                # 如果文件不存在，创建新文件
                new_df = pd.DataFrame([{
                    'analysis.交易币种': symbol,
                    'analysis.方向': direction,
                    'analysis.入场点位1': entry_price,
                    'analysis.止损点位1': stop_loss,
                    'analysis.止盈点位1': target_price,
                    'channel': channel,
                    'timestamp': publish_time
                }])
                if save_to_csv(new_df):
                    logger.info("创建新的CSV文件并写入订单")
                else:
                    logger.error("创建CSV文件失败")
                    return {'status': 'error', 'message': '创建CSV文件失败'}
        except Exception as e:
            logger.error(f"写入CSV文件时出错: {str(e)}")
            return {'status': 'error', 'message': f'保存订单到CSV文件失败: {str(e)}'}
        
        # 添加到内存
        active_orders.append(new_order)
        symbol_key = symbol_upper.replace("USDT", "")
        if symbol_key not in orders_by_symbol:
            orders_by_symbol[symbol_key] = []
        orders_by_symbol[symbol_key].append(new_order)
        
        # 通知前端更新
        serializable_active_orders = make_json_serializable(active_orders)
        socketio.emit('orders_update', {
            'active_orders': serializable_active_orders,
            'timestamp': time.time()
        })
        
        logger.info(f"新订单已添加: {symbol} {direction} 入场:{entry_price} 止损:{stop_loss}")
        return {'status': 'success', 'message': '订单添加成功', 'order_id': new_order['id']}
        
    except Exception as e:
        logger.error(f"添加订单时出错: {str(e)}")
        return {'status': 'error', 'message': f'添加订单失败: {str(e)}'}

@socketio.on('get_csv_status')
def handle_get_csv_status():
    """获取CSV文件状态"""
    try:
        if os.path.exists(csv_file_path):
            file_size = os.path.getsize(csv_file_path) / 1024  # KB
            modification_time = datetime.fromtimestamp(os.path.getmtime(csv_file_path))
            last_check_time = datetime.fromtimestamp(last_csv_check_time)
            
            return {
                'status': 'success',
                'exists': True,
                'file_path': csv_file_path,
                'file_size': f"{file_size:.2f} KB",
                'modification_time': modification_time.strftime('%Y-%m-%d %H:%M:%S'),
                'last_check_time': last_check_time.strftime('%Y-%m-%d %H:%M:%S')
            }
        else:
            return {
                'status': 'info',
                'exists': False,
                'file_path': csv_file_path
            }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def check_csv_updates():
    """检查CSV文件是否有更新，如果有则重新加载数据"""
    global last_csv_check_time, last_csv_modification_time, csv_file_path
    
    current_time = time.time()
    # 每5秒检查一次CSV文件
    if current_time - last_csv_check_time < csv_check_interval:
        return
    
    last_csv_check_time = current_time
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        return
    
    try:
        # 获取文件的最后修改时间
        file_mod_time = os.path.getmtime(csv_file_path)
        
        # 如果文件有更新或者是首次检查
        if file_mod_time > last_csv_modification_time:
            print(f"检测到CSV文件更新: {csv_file_path}")
            last_csv_modification_time = file_mod_time
            
            # 重新加载CSV数据
            load_order_data()
            
            # 通过WebSocket发送更新通知
            if socketio:
                socketio.emit('csv_updated', {
                    'file_path': csv_file_path,
                    'last_modified': datetime.fromtimestamp(file_mod_time).strftime('%Y-%m-%d %H:%M:%S'),
                    'active_orders': len(active_orders),
                    'completed_orders': len(completed_orders)
                })
    except Exception as e:
        print(f"检查CSV文件更新时出错: {e}")

def format_order_for_display(order):
    """
    格式化订单数据，用于WebSocket通信和前端显示
    
    Args:
        order: 订单对象
        
    Returns:
        dict: 格式化后的订单数据
    """
    if not order:
        return None
        
    # 浅拷贝订单数据，避免修改原始数据
    formatted_order = dict(order)
    
    # 确保数值字段是正确的数值类型
    for field in ['profit_pct', 'entry_price', 'current_price', 'target_price', 'stop_loss', 'exit_price']:
        if field in formatted_order and formatted_order[field] is not None:
            try:
                formatted_order[field] = float(formatted_order[field])
            except (ValueError, TypeError):
                formatted_order[field] = None
    
    # 确保profit_pct字段存在
    if 'profit_pct' not in formatted_order or formatted_order['profit_pct'] is None:
        formatted_order['profit_pct'] = 0.0
        
    # 确保current_pnl字段存在
    if 'current_pnl' not in formatted_order:
        formatted_order['current_pnl'] = formatted_order.get('profit_pct', 0.0)
    
    # 安全转换时间格式
    for field in ['publish_time', 'exit_time', 'triggered_time']:
        if field in formatted_order and formatted_order[field]:
            if isinstance(formatted_order[field], datetime):
                formatted_order[field] = formatted_order[field].strftime('%Y-%m-%d %H:%M:%S')
    
    # 添加订单状态指示
    formatted_order['trigger_status'] = 'triggered' if formatted_order.get('triggered', False) else 'waiting'
    
    # 计算潜在盈亏（即使未触发也计算）
    entry_price = formatted_order.get('entry_price')
    current_price = formatted_order.get('current_price')
    
    if entry_price is not None and current_price is not None and entry_price > 0 and current_price > 0:
        if formatted_order.get('direction') == '多':
            potential_pnl = (current_price - entry_price) / entry_price * 100
        else:  # 空单
            potential_pnl = (entry_price - current_price) / entry_price * 100
            
        # 存储潜在盈亏数据，未触发订单显示这个
        formatted_order['potential_pnl'] = potential_pnl
    else:
        formatted_order['potential_pnl'] = 0.0
    
    # 不论是否触发，都添加盈亏状态指示，便于前端展示
    profit = formatted_order.get('profit_pct', 0) if formatted_order.get('triggered', False) else formatted_order.get('potential_pnl', 0)
    if profit > 0:
        formatted_order['profit_status'] = 'positive'
    elif profit < 0:
        formatted_order['profit_status'] = 'negative'
    else:
        formatted_order['profit_status'] = 'neutral'
    
    # 不再清除未触发订单的盈亏数据
    # 而是提供一个标记字段，前端可以根据需要决定显示方式
    formatted_order['is_triggered'] = formatted_order.get('triggered', False)
    
    return formatted_order

def update_price_data(session=None):
    """
    从交易所更新所有支持交易对的价格数据
    
    Args:
        session: 可选的requests会话对象，用于优化HTTP请求
    """
    global price_data, AVAILABLE_SYMBOLS, monitor
    
    # 使用传入的session或创建新的session
    if not session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0'
        })
    
    try:
        # 获取所有支持的交易对
        symbols_to_update = list(AVAILABLE_SYMBOLS.values())
        
        # 使用价格监控器更新所有交易对的价格
        for symbol in symbols_to_update:
            try:
                # 使用monitor的get_price方法获取价格
                price_info = monitor.get_price(symbol)
                
                if price_info and 'mid' in price_info:
                    # 更新价格数据字典
                    price_data[symbol] = {
                        'symbol': symbol,
                        'mid': price_info['mid'],
                        'bid': price_info.get('bid', price_info['mid']),
                        'ask': price_info.get('ask', price_info['mid']),
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'timestamp': price_info.get('timestamp', time.time()),
                        'change_24h': price_info.get('change_24h', 0),
                        'source': price_info.get('source', 'binance')
                    }
            except Exception as e:
                print(f"更新{symbol}价格时出错: {e}")
                
        return True
    except Exception as e:
        print(f"更新价格数据时出错: {e}")
        return False

@app.route('/charts/<path:filename>')
def serve_chart(filename):
    """提供图表文件"""
    try:
        charts_dir = os.path.join(os.path.expanduser('~'), 'Desktop', '交易分析图表')
        if not os.path.exists(charts_dir):
            return jsonify({'error': '图表目录不存在'}), 404
            
        file_path = os.path.join(charts_dir, filename)
        if not os.path.exists(file_path):
            return jsonify({'error': f'找不到文件: {filename}'}), 404
            
        return send_from_directory(charts_dir, filename)
    except Exception as e:
        print(f"提供图表文件时出错: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/charts')
def list_charts():
    """列出所有可用的图表"""
    charts_dir = os.path.join(os.path.expanduser('~'), 'Desktop', '交易分析图表')
    if not os.path.exists(charts_dir):
        return jsonify({'status': 'error', 'message': '图表目录不存在'})
    
    charts = []
    for file in os.listdir(charts_dir):
        if file.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
            charts.append({
                'name': file,
                'url': f'/charts/{file}',
                'type': 'image'
            })
        elif file.endswith(('.html', '.htm')):
            charts.append({
                'name': file,
                'url': f'/charts/{file}',
                'type': 'html'
            })
    
    return jsonify({
        'status': 'success',
        'charts': charts
    })

@app.route('/trade_report')
def trade_report():
    import pandas as pd
    import numpy as np
    import json
    from datetime import datetime
    import traceback
    
    try:
        logger.info("开始处理交易分析报告请求...")
        excel_path = os.path.expanduser('~/Desktop/交易分析报告.xlsx')
        charts_dir = os.path.join(os.path.expanduser('~'), 'Desktop', '交易分析图表')
        
        logger.info(f"Excel文件路径: {excel_path}")
        logger.info(f"图表目录路径: {charts_dir}")
        
        result = {'success': True, 'tables': [], 'images': []}
        
        # 自定义JSON编码器处理特殊值
        class CustomJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                try:
                    if isinstance(obj, (np.integer, np.int64)):
                        return int(obj)
                    elif isinstance(obj, (float, np.float64)):
                        return float(obj) if not np.isnan(obj) else None
                    elif isinstance(obj, (datetime, pd.Timestamp)):
                        return obj.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    return super().default(obj)
                except Exception as e:
                    logger.error(f"JSON编码错误: {str(e)}")
                    return None
        
        # 清理DataFrame中的特殊值
        def clean_dataframe(df):
            try:
                # 替换NaN为None
                df = df.replace({np.nan: None})
                # 转换日期时间列
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                return df
            except Exception as e:
                logger.error(f"清理DataFrame时出错: {str(e)}")
                return df
        
        # 读取所有Sheet表格
        if os.path.exists(excel_path):
            try:
                logger.info("开始读取Excel文件...")
                xl = pd.ExcelFile(excel_path)
                logger.info(f"Excel文件包含以下sheet: {xl.sheet_names}")
                
                for sheet in xl.sheet_names:
                    try:
                        logger.info(f"正在处理sheet: {sheet}")
                        df = xl.parse(sheet)
                        # 清理数据
                        df = clean_dataframe(df)
                        # 转换为列表并处理特殊值
                        rows = []
                        for _, row in df.iterrows():
                            try:
                                row_dict = row.to_dict()
                                # 处理每个值
                                for key, value in row_dict.items():
                                    if pd.isna(value):
                                        row_dict[key] = None
                                    elif isinstance(value, (np.integer, np.int64)):
                                        row_dict[key] = int(value)
                                    elif isinstance(value, (float, np.float64)):
                                        row_dict[key] = float(value) if not np.isnan(value) else None
                                rows.append(row_dict)
                            except Exception as e:
                                logger.error(f"处理行数据时出错: {str(e)}")
                                continue
                        
                        table_data = {
                            'sheet': sheet,
                            'columns': df.columns.tolist(),
                            'rows': rows
                        }
                        result['tables'].append(table_data)
                        logger.info(f"成功处理sheet: {sheet}")
                    except Exception as e:
                        logger.error(f"处理sheet {sheet} 时出错: {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"读取Excel文件时出错: {str(e)}")
                return jsonify({'success': False, 'msg': f'读取Excel文件时出错: {str(e)}'})
        else:
            logger.error(f"Excel文件不存在: {excel_path}")
            return jsonify({'success': False, 'msg': f'找不到文件: {excel_path}'})
        
        # 读取所有图表图片
        if os.path.exists(charts_dir):
            try:
                logger.info("开始读取图表文件...")
                for file in os.listdir(charts_dir):
                    if file.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
                        try:
                            # 从文件名中提取标题
                            title = os.path.splitext(file)[0]  # 移除扩展名
                            title = title.replace('_每日交易分析图', '')  # 移除后缀
                            
                            result['images'].append({
                                'title': title,
                                'url': f'/charts/{file}',
                                'filename': file
                            })
                        except Exception as e:
                            logger.error(f"处理图表文件 {file} 时出错: {str(e)}")
                            continue
                logger.info(f"成功读取 {len(result['images'])} 个图表文件")
            except Exception as e:
                logger.error(f"读取图表文件时出错: {str(e)}")
                return jsonify({'success': False, 'msg': f'读取图表文件时出错: {str(e)}'})
        else:
            logger.error(f"图表目录不存在: {charts_dir}")
            return jsonify({'success': False, 'msg': f'找不到图表目录: {charts_dir}'})
        
        logger.info("所有数据处理完成，准备返回结果...")
        # 使用自定义JSON编码器
        response = jsonify(result)
        response.headers['Content-Type'] = 'application/json'
        return response
        
    except Exception as e:
        logger.error(f"处理交易分析报告时出错: {str(e)}")
        logger.error("错误详情:")
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'msg': f'处理交易分析报告时出错: {str(e)}'})

@app.route('/trade_analysis_data')
def trade_analysis_data():
    """读取交易分析报告并返回分析数据"""
    try:
        # 读取Excel文件
        excel_path = os.path.expanduser('~/Desktop/交易分析报告.xlsx')
        print(f"尝试读取Excel文件: {excel_path}")
        
        if not os.path.exists(excel_path):
            print(f"Excel文件不存在: {excel_path}")
            return jsonify({
                'success': False,
                'msg': '找不到交易分析报告文件'
            })

        # 读取各个Sheet
        print("开始读取Excel文件...")
        with pd.ExcelFile(excel_path) as xl:
            print(f"Excel文件包含以下sheet: {xl.sheet_names}")
            
            # 1. 读取总体统计
            print("读取总体统计sheet...")
            summary_df = pd.read_excel(xl, sheet_name='总体统计')
            summary = summary_df.iloc[0].to_dict()
            print(f"总体统计数据: {summary}")

            # 2. 读取每日收益率总结表
            print("读取每日收益率总结表sheet...")
            daily_df = pd.read_excel(xl, sheet_name='每日收益率总结表')
            daily = daily_df.to_dict('records')
            print(f"每日收益率数据条数: {len(daily)}")

            # 3. 读取详细交易（可选）
            print("读取详细交易sheet...")
            trades_df = pd.read_excel(xl, sheet_name='详细交易')
            trades = trades_df.head(100).to_dict('records')
            print(f"详细交易数据条数: {len(trades)}")

        # 4. 获取图表文件列表
        charts_dir = os.path.join(os.path.expanduser('~'), 'Desktop', '交易分析图表')
        print(f"查找图表文件目录: {charts_dir}")
        charts = []
        if os.path.exists(charts_dir):
            for file in os.listdir(charts_dir):
                if file.endswith(('.png', '.jpg', '.jpeg')):
                    charts.append({
                        'title': os.path.splitext(file)[0],
                        'url': f'/charts/{file}'
                    })
            print(f"找到 {len(charts)} 个图表文件")

        # 5. 返回完整数据
        response_data = {
            'success': True,
            'summary': summary,
            'daily': daily,
            'trades': trades,
            'charts': charts
        }
        print("准备返回数据...")
        return jsonify(response_data)

    except Exception as e:
        print(f"读取分析数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'msg': f'读取分析数据时出错: {str(e)}'
        })

ADMIN_PASSWORD = "1234"  # 请替换为你自己的密码

@app.route('/orders')
def get_orders():
    """获取活跃订单或已完成订单，让客户端处理搜索和分页"""
    order_type = request.args.get('type', 'active')
    try:
        # 获取DataTables发送的参数
        draw = int(request.args.get('draw', 1))
    except Exception as e:
        logger.error(f"处理订单请求参数时出错: {e}")
        draw = 1

    # 获取原始订单数据
    if order_type == 'active':
        orders = active_orders
    else:
        orders = completed_orders
    
    total_records = len(orders)
    
    # 确保JSON可序列化
    orders = make_json_serializable(orders)

    return jsonify({
        'data': orders,
        'recordsTotal': total_records,
        'recordsFiltered': total_records,
        'draw': draw,
    })

# 在文件开头添加CSV文件路径配置
import os
from pathlib import Path

# 定义CSV文件路径
def get_csv_file_path():
    """获取CSV文件的绝对路径，并确保目录存在"""
    try:
        # 使用用户桌面目录
        desktop_path = os.path.expanduser('~/Desktop')
        # 在桌面创建data目录
        data_dir = os.path.join(desktop_path, 'discord-monitor-data')
        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)
        # 返回CSV文件的完整路径
        csv_path = os.path.join(data_dir, 'all_analysis_results.csv')
        
        # 检查文件权限
        if os.path.exists(csv_path):
            # 检查文件是否可写
            if not os.access(csv_path, os.W_OK):
                logger.error(f"CSV文件没有写入权限: {csv_path}")
                # 尝试修改文件权限
                try:
                    os.chmod(csv_path, 0o666)  # 给予读写权限
                    logger.info(f"已修改CSV文件权限: {csv_path}")
                except Exception as e:
                    logger.error(f"无法修改CSV文件权限: {str(e)}")
                    # 尝试使用管理员权限
                    try:
                        import ctypes
                        if os.name == 'nt':  # Windows系统
                            ctypes.windll.shell32.ShellExecuteW(None, "runas", "cmd.exe", f'/c icacls "{csv_path}" /grant Everyone:F', None, 1)
                            logger.info("已尝试使用管理员权限修改文件权限")
                    except Exception as e:
                        logger.error(f"使用管理员权限修改文件权限失败: {str(e)}")
        else:
            # 如果文件不存在，创建一个空文件
            try:
                with open(csv_path, 'w', encoding='utf-8') as f:
                    f.write("timestamp,analysis.交易币种,analysis.方向,analysis.入场点位1,analysis.止损点位1,analysis.止盈点位1,channel\n")
                # 设置文件权限
                os.chmod(csv_path, 0o666)
                logger.info(f"已创建新的CSV文件: {csv_path}")
            except Exception as e:
                logger.error(f"创建CSV文件失败: {str(e)}")
                # 尝试使用管理员权限创建
                try:
                    import ctypes
                    if os.name == 'nt':  # Windows系统
                        temp_path = os.path.join(os.environ['TEMP'], 'temp_csv.csv')
                        with open(temp_path, 'w', encoding='utf-8') as f:
                            f.write("timestamp,analysis.交易币种,analysis.方向,analysis.入场点位1,analysis.止损点位1,analysis.止盈点位1,channel\n")
                        ctypes.windll.shell32.ShellExecuteW(None, "runas", "cmd.exe", f'/c move /Y "{temp_path}" "{csv_path}"', None, 1)
                        logger.info("已尝试使用管理员权限创建文件")
                except Exception as e:
                    logger.error(f"使用管理员权限创建文件失败: {str(e)}")
        
        return csv_path
    except Exception as e:
        logger.error(f"获取CSV文件路径时出错: {str(e)}")
        return None

def save_to_csv(df):
    """安全地保存DataFrame到CSV文件"""
    global csv_file_path
    
    try:
        if csv_file_path is None:
            logger.error("CSV文件路径无效")
            return False
            
        # 确保目录存在
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        # 尝试保存文件
        temp_path = os.path.join(os.environ['TEMP'], 'temp_csv.csv')
        df.to_csv(temp_path, index=False, encoding='utf-8')
        
        # 如果临时文件保存成功，替换原文件
        if os.path.exists(temp_path):
            try:
                # 尝试直接移动文件
                if os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
                os.rename(temp_path, csv_file_path)
            except Exception as e:
                logger.error(f"移动文件失败，尝试使用管理员权限: {str(e)}")
                try:
                    # 尝试使用管理员权限移动文件
                    import ctypes
                    if os.name == 'nt':  # Windows系统
                        ctypes.windll.shell32.ShellExecuteW(None, "runas", "cmd.exe", f'/c move /Y "{temp_path}" "{csv_file_path}"', None, 1)
                        logger.info("已尝试使用管理员权限移动文件")
                except Exception as e:
                    logger.error(f"使用管理员权限移动文件失败: {str(e)}")
                    return False
            
            # 设置文件权限
            try:
                os.chmod(csv_file_path, 0o666)
            except Exception as e:
                logger.error(f"设置文件权限失败: {str(e)}")
            
            logger.info(f"成功保存CSV文件: {csv_file_path}")
            return True
        else:
            logger.error("保存临时文件失败")
            return False
            
    except Exception as e:
        logger.error(f"保存CSV文件时出错: {str(e)}")
        # 清理临时文件
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return False

# 在程序启动时添加权限检查
def check_file_permissions():
    """检查并确保文件权限正确"""
    global csv_file_path
    
    try:
        if csv_file_path and os.path.exists(csv_file_path):
            # 检查文件权限
            if not os.access(csv_file_path, os.W_OK):
                logger.warning(f"CSV文件没有写入权限，尝试修复: {csv_file_path}")
                try:
                    # 尝试修改文件权限
                    os.chmod(csv_file_path, 0o666)
                    logger.info("已修改文件权限")
                except Exception as e:
                    logger.error(f"修改文件权限失败: {str(e)}")
                    # 尝试使用管理员权限
                    try:
                        import ctypes
                        if os.name == 'nt':  # Windows系统
                            ctypes.windll.shell32.ShellExecuteW(None, "runas", "cmd.exe", f'/c icacls "{csv_file_path}" /grant Everyone:F', None, 1)
                            logger.info("已尝试使用管理员权限修改文件权限")
                    except Exception as e:
                        logger.error(f"使用管理员权限修改文件权限失败: {str(e)}")
    except Exception as e:
        logger.error(f"检查文件权限时出错: {str(e)}")

def initialize_csv_file():
    """初始化CSV文件路径和权限"""
    global csv_file_path
    try:
        # 获取CSV文件路径
        csv_file_path = get_csv_file_path()
        if csv_file_path is None:
            logger.error("无法初始化CSV文件路径")
            return False
            
        # 检查文件权限
        check_file_permissions()
        
        # 确保文件存在
        if not os.path.exists(csv_file_path):
            try:
                with open(csv_file_path, 'w', encoding='utf-8') as f:
                    f.write("timestamp,analysis.交易币种,analysis.方向,analysis.入场点位1,analysis.止损点位1,analysis.止盈点位1,channel\n")
                os.chmod(csv_file_path, 0o666)
                logger.info(f"已创建新的CSV文件: {csv_file_path}")
            except Exception as e:
                logger.error(f"创建CSV文件失败: {str(e)}")
                return False
        
        logger.info(f"CSV文件初始化成功: {csv_file_path}")
        return True
    except Exception as e:
        logger.error(f"初始化CSV文件时出错: {str(e)}")
        return False

if __name__ == '__main__':
    # 禁用所有非关键日志
    import logging
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    
    # 仅保留警告和错误级别的日志，但禁用其他
    for logger_name in ['socketio', 'engineio', 'werkzeug', 'geventwebsocket', 'flask', 'websocket']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)
    
    # 初始化CSV文件路径
    csv_file_path = get_csv_file_path()
    
    # 初始化CSV文件
    if not initialize_csv_file():
        logger.error("CSV文件初始化失败，程序可能无法正常工作")
        print("警告：CSV文件初始化失败，程序可能无法正常工作")
    
    # 打印欢迎信息和ASCII艺术标题
    print("\n")
    print("=" * 60)
    print("""
    ██████╗ ██████╗ ██╗ ██████╗███████╗    ███╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗ 
    ██╔══██╗██╔══██╗██║██╔════╝██╔════╝    ████╗ ████║██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗
    ██████╔╝██████╔╝██║██║     █████╗      ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝
    ██╔═══╝ ██╔══██╗██║██║     ██╔══╝      ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗
    ██║     ██║  ██║██║╚██████╗███████╗    ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ██║
    ╚═╝     ╚═╝  ╚═╝╚═╝ ╚═════╝╚══════╝    ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
    """)
    print("=" * 60)
    print("价格订单监控系统 - 启动中...")
    print("=" * 60)
    
    # 获取系统信息
    import platform
    import psutil
    
    try:
        print(f"系统信息:")
        print(f"  操作系统: {platform.system()} {platform.release()}")
        print(f"  Python版本: {platform.python_version()}")
        
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        print(f"  CPU使用率: {cpu_percent}%")
        print(f"  内存使用: {memory.percent}% (总计: {memory.total / (1024**3):.1f} GB)")
        print(f"  磁盘空间: 已用 {disk.percent}% (可用: {disk.free / (1024**3):.1f} GB)")
    except ImportError:
        print("无法获取完整系统信息，继续启动...")
    except Exception as e:
        print(f"获取系统信息时出错: {e}")
    
    print("=" * 60 + "\n")
    
    # 加载订单数据
    print("正在加载订单数据...")
    load_order_data()
    print("订单数据加载完成")
    
    # 检查CSV文件
    if csv_file_path:
        print(f"检查CSV文件: {csv_file_path}")
        try:
            if os.path.exists(csv_file_path):
                file_size = os.path.getsize(csv_file_path) / 1024  # KB
                modification_time = datetime.fromtimestamp(os.path.getmtime(csv_file_path))
                print(f"  CSV文件存在: 大小 {file_size:.2f} KB, 最后修改时间: {modification_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 保存文件修改时间
                last_csv_modification_time = os.path.getmtime(csv_file_path)
                
                # 尝试从CSV文件加载初始数据
                print("  正在从CSV文件加载初始订单数据...")
                initial_load = monitor_csv_file()
                if initial_load:
                    print(f"  成功从CSV文件加载初始订单数据，活跃订单: {len(active_orders)}个")
                else:
                    print("  未从CSV文件中找到符合条件的初始订单数据")
            else:
                print(f"  CSV文件不存在，将创建一个空文件用于后续监控")
                with open(csv_file_path, 'w') as f:
                    f.write("timestamp,analysis.交易币种,analysis.方向,analysis.入场点位1,analysis.止损点位1,analysis.止盈点位1\n")
                    
                last_csv_modification_time = os.path.getmtime(csv_file_path)
                print(f"  创建了空CSV文件")
        except Exception as e:
            print(f"检查CSV文件时出错: {e}")
    else:
        print("警告：CSV文件路径未初始化")
    
    # 设置CSV检查时间
    last_csv_check_time = time.time()
    
    # 启动监控线程
    monitor.keep_running = True
    price_thread = socketio.start_background_task(background_monitoring)
    
    # 设置主机和端口
    host = '0.0.0.0'  # 绑定到所有网络接口，允许外部访问
    port = 8080  # 使用8080端口
    
    # 打印访问地址
    print("\n=================================================")
    print(f"应用已启动，可通过以下地址访问：")
    print(f"本地访问: http://localhost:{port}")
    print(f"外部访问: http://47.239.197.28/")
    print("=================================================\n")
    
    # 只过滤订单数据，允许其他简单输出
    import sys
    import re
    original_stdout = sys.stdout
    
    class SimpleFilteredStdout:
        def __init__(self, original):
            self.original = original
            # 添加订单数据关键字的模式
            self.blacklist_patterns = [
                r'"id"\s*:\s*\d+',                       # ID模式
                r'"symbol"\s*:\s*"[^"]*"',               # 币种模式
                r'"direction"\s*:\s*"[^"]*"',            # 方向模式
                r'"entry_price"\s*:',                    # 入场价格模式
                r'"profit_pct"\s*:',                     # 收益百分比模式
                r'"target_price"\s*:',                   # 目标价格模式
                r'"stop_loss"\s*:',                      # 止损模式
                r'"channel"\s*:',                        # 频道模式
                r'"normalized_symbol"\s*:'               # 标准化交易对模式
            ]
            self.line_threshold = 200  # 超过这个长度的行将被检查是否包含大量括号和逗号
        
        def write(self, message):
            # 只过滤长消息，让短小的状态信息通过
            if len(message) > self.line_threshold:
                # 检查是否为JSON数组或对象 (包含大量的{},[]和,)
                json_chars = sum(message.count(c) for c in "{}[],:")
                if json_chars > 10:  # 如果包含多个JSON特征字符
                    # 进一步检查是否包含订单数据特征
                    if any(re.search(pattern, message) for pattern in self.blacklist_patterns):
                        # 替换为简单提示
                        if "active_orders" in message or "completed_orders" in message:
                            self.original.write("[订单数据已被过滤，不显示具体内容]\n")
                        return
            
            # 通过检查的消息正常输出
            self.original.write(message)
        
        def flush(self):
            self.original.flush()
    
    # 替换标准输出
    # sys.stdout = SimpleFilteredStdout(original_stdout)
    
    # 启动Flask应用，只允许本地访问
    print("准备启动Web服务器...")
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)