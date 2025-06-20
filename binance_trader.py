# -*- coding: utf-8 -*-
import os
import time
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union
from decimal import Decimal
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.enums import *
import glob
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

# 硬编码配置信息
BINANCE_API_KEY = "VIJFNRp99K0dvJ7GEZFsnCVbuLhruo2H1Kh4xADeviXjegoV0NQKzFv8I2cSMcvF"
BINANCE_API_SECRET = "jmEqAmrFYOhfnT65YFOvxn6nhOQ7TYw9pJlGltOY0xxDCyrZDrSQdiK9z4Yygc9a"

# 交易配置
TRADING_CONFIG = {
    'position_size': 50,  # 固定交易金额（USDT）
    'leverage': 20,       # 杠杆倍数
    'margin_type': 'CROSSED',  # 保证金类型：ISOLATED(逐仓) 或 CROSSED(全仓)
}

# 支持的交易对及其精度
SUPPORTED_SYMBOLS = {}  # 将由get_all_supported_symbols方法动态填充

class BinanceTrader:
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        初始化币安交易客户端
        
        Args:
            api_key: 币安API Key
            api_secret: 币安API Secret
        """
        # 使用硬编码的API密钥
        self.api_key = api_key or BINANCE_API_KEY
        self.api_secret = api_secret or BINANCE_API_SECRET
        
        if not self.api_key or not self.api_secret:
            raise ValueError("必须提供币安API密钥和密钥")
        
        # 初始化币安客户端
        self.client = Client(self.api_key, self.api_secret)
        
        # 初始化BTC仓位配置
        self.btc_initial_capital = 1000  # 初始资金1000U
        self.btc_leverage = 60  # 60倍杠杆
        self.btc_position_file = os.path.join(os.path.expanduser('~'), 'Desktop', 'btc仓位.xlsx')  # 修改为.xlsx文件
        self.btc_channel_positions = self.load_btc_position_config()
        
        # 初始化时间偏移量
        self.time_offset = 0
        
        # 同步服务器时间（最多重试3次）
        for _ in range(3):
            try:
                server_time = self.client.get_server_time()
                local_time = int(time.time() * 1000)
                self.time_offset = server_time['serverTime'] - local_time
                logger.info(f"服务器时间差: {self.time_offset}ms")
                if abs(self.time_offset) > 1000:  # 如果时间差超过1秒
                    logger.warning(f"系统时间与服务器时间不同步，已自动调整时间差")
                break
            except Exception as e:
                logger.error(f"同步服务器时间失败: {e}")
                time.sleep(1)  # 等待1秒后重试
        
        # 使用硬编码的交易配置
        self.trading_config = TRADING_CONFIG.copy()
        
        # 初始化交易状态
        self.active_orders = {}
        self.position_info = {}
        
        # 分析结果文件路径
        self.analysis_file = os.path.join('C:\\', 'Users', 'Administrator', 'Desktop', 'Discord', 'data', 'analysis_results', 'all_analysis_results.csv')
        
        # 已执行订单记录文件
        self.executed_orders_file = os.path.join('C:\\', 'Users', 'Administrator', 'Desktop', 'Discord', 'data', 'executed_orders.json')
        
        # 订单配对关系文件
        self.order_pairs_file = os.path.join('C:\\', 'Users', 'Administrator', 'Desktop', 'Discord', 'data', 'order_pairs.json')
        
        # 加载已执行的订单记录
        self.executed_signals = self.load_executed_signals()
        
        # 加载订单配对关系
        self.order_pairs = self.load_order_pairs()
        
        # 清理过期的执行记录
        self.clean_expired_signals()
        
        # 获取所有支持的交易对信息
        self.supported_symbols = self.get_all_supported_symbols()
        
        # 检查账户持仓情况
        try:
            # 检查当前持仓模式
            position_mode = self._request(self.client.futures_get_position_mode)
            is_hedge_mode = position_mode['dualSidePosition']
            
            # 设置BTC和ETH为双向持仓，其他币种为单向持仓
            if is_hedge_mode:
                # 如果当前是对冲模式，先关闭对冲模式
                self._request(self.client.futures_change_position_mode, dualSidePosition=False)
                logger.info("已关闭对冲模式")
            
            # 为BTC和ETH单独设置对冲模式
            for symbol in ['BTCUSDT', 'ETHUSDT']:
                try:
                    self._request(self.client.futures_change_position_mode, dualSidePosition=True)
                    logger.info(f"已为{symbol}设置对冲模式")
                except Exception as e:
                    logger.error(f"设置{symbol}对冲模式失败: {e}")
            
            logger.info(f"当前持仓模式: {'对冲模式' if is_hedge_mode else '单向持仓'}")
            
            # 设置保证金类型和杠杆
            for symbol_info in self.supported_symbols.values():
                try:
                    symbol = symbol_info['symbol']
                    # 先设置保证金类型
                    self._request(self.client.futures_change_margin_type, symbol=symbol, marginType=self.trading_config['margin_type'])
                    logger.info(f"已设置{symbol}保证金类型为{self.trading_config['margin_type']}")
                    
                    # 再设置杠杆倍数
                    if symbol in ['BTCUSDT', 'ETHUSDT']:
                        self._request(self.client.futures_change_leverage, symbol=symbol, leverage=self.btc_leverage)
                        logger.info(f"已设置{symbol}杠杆倍数为{self.btc_leverage}倍")
                    else:
                        self._request(self.client.futures_change_leverage, symbol=symbol, leverage=self.trading_config['leverage'])
                        logger.info(f"已设置{symbol}杠杆倍数为{self.trading_config['leverage']}倍")
                        
                except BinanceAPIException as e:
                    if e.code not in [-4046, -4047]:  # 忽略"已经是目标类型/倍数"的错误
                        raise
                    else:
                        logger.info(f"保证金类型或杠杆倍数已经是目标值: {e}")
                        
        except Exception as e:
            logger.error(f"设置账户持仓模式失败: {e}")
        
        logger.info("币安合约交易客户端初始化完成")
    
    def load_executed_signals(self) -> Dict:
        """
        从文件加载已执行的订单记录
        
        Returns:
            Dict: 已执行的订单记录字典，格式为 {signal_key: execution_time}
        """
        try:
            if os.path.exists(self.executed_orders_file):
                with open(self.executed_orders_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 将列表转换为字典，如果没有时间戳则使用当前时间
                    signals_dict = {}
                    for item in data:
                        if isinstance(item, dict):
                            signals_dict[item['signal_key']] = item.get('execution_time', time.time())
                        else:
                            signals_dict[item] = time.time()
                    logger.info(f"已加载 {len(signals_dict)} 条已执行订单记录")
                    return signals_dict
            return {}
        except Exception as e:
            logger.error(f"加载已执行订单记录失败: {e}")
            return {}

    def save_executed_signals(self):
        """
        保存已执行的订单记录到文件
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.executed_orders_file), exist_ok=True)
            
            # 将字典转换为列表格式
            data = [{'signal_key': key, 'execution_time': value} for key, value in self.executed_signals.items()]
            
            # 保存记录
            with open(self.executed_orders_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(self.executed_signals)} 条已执行订单记录")
        except Exception as e:
            logger.error(f"保存已执行订单记录失败: {e}")

    def get_account_info(self) -> Dict:
        """
        获取账户信息
        
        Returns:
            Dict: 账户信息
        """
        try:
            account = self._request(self.client.futures_account)
            if not account:
                logger.error("无法获取账户信息")
                return {}
                
            # 获取USDT余额
            assets = account.get('assets', [])
            usdt_asset = next((asset for asset in assets if asset['asset'] == 'USDT'), None)
            
            if not usdt_asset:
                logger.error("无法获取USDT资产信息")
                return {}
                
            # 计算可用余额
            available_balance = float(usdt_asset.get('availableBalance', 0))
            wallet_balance = float(usdt_asset.get('walletBalance', 0))
            
            logger.info(f"账户信息:\n  钱包余额: {wallet_balance:.2f} USDT\n  可用余额: {available_balance:.2f} USDT")
            
            return {
                'availableBalance': available_balance,
                'walletBalance': wallet_balance
            }
            
        except Exception as e:
            logger.error(f"获取账户信息失败: {e}")
            return {}

    def get_balance(self, asset: str = 'USDT') -> float:
        """
        获取指定资产的合约余额
        
        Args:
            asset: 资产名称，默认USDT
            
        Returns:
            float: 余额
        """
        try:
            account = self._request(self.client.futures_account)
            for balance in account['assets']:
                if balance['asset'] == asset:
                    return float(balance['walletBalance'])
            return 0.0
        except BinanceAPIException as e:
            logger.error(f"获取{asset}合约余额失败: {e}")
            return 0.0
    
    def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取合约交易对信息
        
        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            
        Returns:
            Dict: 交易对信息
        """
        try:
            info = self._request(self.client.futures_exchange_info)
            for symbol_info in info['symbols']:
                if symbol_info['symbol'] == symbol:
                    return symbol_info
            return {}
        except BinanceAPIException as e:
            logger.error(f"获取{symbol}合约交易对信息失败: {e}")
            return {}
    
    def get_current_price(self, symbol: str) -> float:
        """
        获取当前价格
        
        Args:
            symbol: 交易对符号
            
        Returns:
            float: 当前价格
        """
        try:
            # 确保使用正确的交易对符号
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
            
            # 移除可能的后缀（如_250926）
            base_symbol = symbol.split('_')[0]
                
            # 使用期货API获取价格
            ticker = self._request(self.client.futures_symbol_ticker, symbol=base_symbol)
            price = float(ticker['price'])
            
            # 检查价格有效性
            if not price or price <= 0:
                logger.error(f"获取到{symbol}无效价格: {price}")
                return None
                
            return price
        except Exception as e:
            logger.error(f"获取{symbol}当前价格失败: {e}")
            return None

    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """
        获取未完成订单
        
        Args:
            symbol: 交易对符号
            
        Returns:
            List[Dict]: 未完成订单列表
        """
        try:
            # 确保使用正确的交易对符号
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
                
            return self._request(self.client.futures_get_open_orders, symbol=symbol)
        except Exception as e:
            logger.error(f"获取未完成订单失败: {e}")
            return []

    def format_quantity(self, symbol: str, quantity: float) -> float:
        """
        格式化交易数量，确保符合币安精度要求
        
        Args:
            symbol: 交易对符号
            quantity: 原始数量
            
        Returns:
            float: 格式化后的数量
        """
        try:
            # 确保使用正确的交易对符号
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
                
            logger.info(f"开始格式化数量: symbol={symbol}, quantity={quantity}")
            
            # 获取交易对信息
            symbol_info = None
            for key, value in SUPPORTED_SYMBOLS.items():
                if value['symbol'] == symbol:
                    symbol_info = value
                    break
            
            if not symbol_info:
                raise ValueError(f"不支持的交易对: {symbol}")
                
            logger.info(f"找到交易对信息: {symbol_info}")
            
            # 获取当前价格
            current_price = self.get_current_price(symbol)
            if not current_price:
                raise ValueError(f"无法获取{symbol}当前价格")
                
            logger.info(f"当前价格: {current_price}")
            
            # 格式化数量
            precision = symbol_info['quantity_precision']
            min_qty = symbol_info['min_qty']
            
            logger.info(f"数量精度: {precision}, 最小数量: {min_qty}")
            
            # 确保数量不小于最小交易量
            if quantity < min_qty:
                logger.warning(f"数量 {quantity} 小于最小交易量 {min_qty}，将使用最小交易量")
                quantity = min_qty
            
            # 根据精度格式化
            if precision == 0:
                # 如果是整数精度，直接取整
                formatted_qty = int(quantity)
            else:
                formatted_qty = float(f"{{:.{precision}f}}".format(quantity))
                
            logger.info(f"格式化后的数量: {formatted_qty}")
            
            # 验证名义金额是否满足要求
            notional = formatted_qty * current_price
            logger.info(f"计算的名义金额: {notional} USDT")
            
            if notional < 100:
                # 如果名义金额小于100，增加数量
                formatted_qty = 100 / current_price
                if precision == 0:
                    formatted_qty = int(formatted_qty)
                else:
                    formatted_qty = float(f"{{:.{precision}f}}".format(formatted_qty))
                logger.info(f"调整交易数量以满足最小名义金额要求: {formatted_qty}")
                
                # 重新计算名义金额
                notional = formatted_qty * current_price
                logger.info(f"调整后的名义金额: {notional} USDT")
            
            # 最终验证
            if formatted_qty <= 0:
                raise ValueError(f"格式化后的数量无效: {formatted_qty}")
                
            if notional < 100:
                raise ValueError(f"名义金额 {notional} USDT 小于最小要求 100 USDT")
                
            logger.info(f"最终格式化结果: quantity={formatted_qty}, notional={notional} USDT")
            return formatted_qty
            
        except Exception as e:
            logger.error(f"格式化数量时出错: {e}")
            raise

    def format_price(self, symbol: str, price: float) -> float:
        """
        格式化价格，确保符合币安精度要求
        
        Args:
            symbol: 交易对符号
            price: 原始价格
            
        Returns:
            float: 格式化后的价格
        """
        try:
            # 确保使用正确的交易对符号
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
                
            # 验证价格
            if not price or price <= 0:
                raise ValueError(f"价格必须大于0: {price}")
                
            # 获取交易对信息
            symbol_info = None
            for key, value in SUPPORTED_SYMBOLS.items():
                if value['symbol'] == symbol:
                    symbol_info = value
                    break
            
            if not symbol_info:
                logger.warning(f"未找到交易对 {symbol} 的精度信息，使用默认精度4")
                precision = 4
            else:
                precision = symbol_info['price_precision']
            
            # 格式化价格
            formatted_price = float(f"{{:.{precision}f}}".format(price))
            
            # 验证格式化后的价格
            if not formatted_price or formatted_price <= 0:
                raise ValueError(f"格式化后的价格无效: {formatted_price}")
                
            logger.info(f"价格格式化: 原始价格={price}, 格式化后={formatted_price}, 精度={precision}")
            return formatted_price
            
        except Exception as e:
            logger.error(f"格式化价格时出错: {e}")
            return price  # 出错时返回原始价格

    def place_order(self, 
                   symbol: str, 
                   side: str, 
                   order_type: str, 
                   quantity: float = None,
                   price: float = None,
                   stop_price: float = None,
                   time_in_force: str = 'GTC',
                   notional: float = None,
                   reduceOnly: bool = False) -> Dict:
        """
        下合约单
        
        Args:
            symbol: 交易对符号
            side: 方向 (BUY/SELL)
            order_type: 订单类型
            quantity: 数量（可选）
            price: 价格（限价单需要）
            stop_price: 触发价格（止损/止盈单需要）
            time_in_force: 订单有效期
            notional: 名义金额（可选）
            reduceOnly: 是否只减仓（可选）
            
        Returns:
            Dict: 订单信息
        """
        try:
            logger.info(f"准备下单: {symbol} {side} {order_type}")
            
            # 检查当前持仓模式
            position_mode = self._request(self.client.futures_get_position_mode)
            is_hedge_mode = position_mode['dualSidePosition']
            logger.info(f"当前持仓模式: {'对冲模式' if is_hedge_mode else '单向持仓'}")
            
            # 确保保证金类型和杠杆设置正确
            try:
                # 先设置保证金类型
                self._request(self.client.futures_change_margin_type, symbol=symbol, marginType=self.trading_config['margin_type'])
                logger.info(f"已设置{symbol}保证金类型为{self.trading_config['margin_type']}")
                
                # 再设置杠杆倍数
                self._request(self.client.futures_change_leverage, symbol=symbol, leverage=self.trading_config['leverage'])
                logger.info(f"已设置{symbol}杠杆倍数为{self.trading_config['leverage']}倍")
                
            except BinanceAPIException as e:
                if e.code not in [-4046, -4047]:  # 忽略"已经是目标类型/倍数"的错误
                    raise
                else:
                    logger.info(f"保证金类型或杠杆倍数已经是目标值: {e}")
            
            # 获取交易对信息
            symbol_info = None
            for key, value in self.supported_symbols.items():
                if value['symbol'] == symbol:
                    symbol_info = value
                    break
            
            if not symbol_info:
                logger.error(f"交易对 {symbol} 不在支持列表中")
                raise ValueError(f"不支持的交易对: {symbol}")
            
            logger.info(f"找到交易对信息: {symbol_info}")
            
            # 格式化价格
            if price:
                price = self.format_price(symbol, price)
                logger.info(f"格式化后的价格: {price}")
            if stop_price:
                stop_price = self.format_price(symbol, stop_price)
                logger.info(f"格式化后的止损价格: {stop_price}")
            # 如果 price 没有传递或为0，自动用当前市场价兜底
            if not price or price == 0:
                price = self.get_current_price(symbol)
                logger.warning(f"未传递价格，自动使用当前市场价: {price}")
            
            # 如果是止损单或止盈单，检查价格是否合适
            if order_type in ['STOP_MARKET', 'TAKE_PROFIT_MARKET', 'STOP_LIMIT', 'TAKE_PROFIT_LIMIT']:
                if not stop_price:
                    raise ValueError(f"{order_type}订单必须提供stopPrice参数")
                
                # 获取当前市场价格
                current_price = self.get_current_price(symbol)
                if not current_price:
                    raise ValueError(f"无法获取{symbol}当前价格")
                
                # 检查止损价格是否合适
                price_diff_percent = abs(stop_price - current_price) / current_price
                if price_diff_percent < 0.001:  # 如果价格差异小于0.1%
                    raise ValueError(f"止损价格 {stop_price} 太接近当前价格 {current_price}，差异: {price_diff_percent*100:.2f}%")
                
                # 检查止损价格方向是否正确
                if side == 'BUY' and stop_price >= current_price:
                    raise ValueError(f"买单的止损价格 {stop_price} 不能高于当前价格 {current_price}")
                elif side == 'SELL' and stop_price <= current_price:
                    raise ValueError(f"卖单的止损价格 {stop_price} 不能低于当前价格 {current_price}")
            
            # 构建订单参数
            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type
            }
            
            # 根据持仓模式设置positionSide
            if is_hedge_mode:
                # 对冲模式下，需要设置positionSide
                params['positionSide'] = 'LONG' if side == 'BUY' else 'SHORT'
                logger.info(f"对冲模式，设置positionSide: {params['positionSide']}")
            else:
                # 非对冲模式下，才设置reduceOnly
                params['reduceOnly'] = reduceOnly
            
            # 如果是止损单或止盈单，添加stopPrice参数
            if order_type in ['STOP_MARKET', 'TAKE_PROFIT_MARKET', 'STOP_LIMIT', 'TAKE_PROFIT_LIMIT']:
                params['stopPrice'] = stop_price
                params['workingType'] = 'MARK_PRICE'  # 使用标记价格作为触发价格
            
            # 如果提供了名义金额，使用名义金额下单
            if notional:
                # 获取当前价格用于计算数量
                current_price = self.get_current_price(symbol)
                if not current_price or current_price <= 0:  # 添加除零检查
                    raise ValueError(f"无法获取{symbol}当前价格或价格无效: {current_price}")
                
                try:
                    # 计算数量
                    quantity = notional / current_price
                    if not quantity or quantity <= 0:
                        raise ValueError(f"计算出的数量无效: {quantity}")
                        
                    quantity = self.format_quantity(symbol, quantity)
                    logger.info(f"使用当前价格 {current_price} 计算数量: {quantity}")
                    
                    # 确保quantity参数被正确设置
                    params['quantity'] = quantity
                    
                    # 根据订单类型设置不同的参数
                    if order_type in ['STOP_LIMIT', 'TAKE_PROFIT']:
                        params['type'] = order_type
                        params['stopPrice'] = stop_price
                        params['workingType'] = 'MARK_PRICE'
                        params['price'] = price
                    else:
                        if not price:
                            raise ValueError("使用名义金额下单时必须提供价格")
                        params['type'] = 'LIMIT'
                        params['timeInForce'] = 'GTC'
                        params['price'] = price
                        
                    # 计算实际保证金
                    actual_margin = notional / self.trading_config['leverage']
                    logger.info(f"名义金额: {notional} USDT, 实际保证金: {actual_margin} USDT")
                    
                except ZeroDivisionError:
                    raise ValueError(f"计算数量时发生除零错误，当前价格: {current_price}")
                except Exception as e:
                    raise ValueError(f"计算数量时发生错误: {str(e)}")
            else:
                # 否则使用数量下单
                if not quantity:
                    raise ValueError("必须提供quantity或notional参数")
                quantity = self.format_quantity(symbol, quantity)
                if not quantity or quantity <= 0:
                    raise ValueError(f"格式化后的数量无效: {quantity}")
                    
                params['quantity'] = quantity
                if order_type in ['LIMIT', 'STOP_LIMIT', 'TAKE_PROFIT']:
                    params['timeInForce'] = time_in_force
                    params['price'] = price
                    if order_type in ['STOP_LIMIT', 'TAKE_PROFIT']:
                        params['stopPrice'] = stop_price
                        params['workingType'] = 'MARK_PRICE'
            
            logger.info(f"最终订单参数: {params}")
            
            # 下单
            order = self._request(self.client.futures_create_order, **params)
            logger.info(f"下合约单成功: {symbol} {side} {order_type} {params.get('quantity')}")
            return order
            
        except Exception as e:
            logger.error(f"下合约单失败: {e}")
            raise
    
    def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """
        取消合约订单
        
        Args:
            symbol: 交易对名称
            order_id: 订单ID
            
        Returns:
            Dict: 取消结果
        """
        try:
            result = self._request(self.client.futures_cancel_order, symbol=symbol, orderId=order_id)
            
            # 从活跃订单中移除
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            
            logger.info(f"取消合约订单成功: {symbol} {order_id}")
            return result
            
        except BinanceAPIException as e:
            logger.error(f"取消合约订单失败: {e}")
            return {}
    
    def get_order_status(self, symbol: str, order_id: int) -> Dict:
        """
        获取合约订单状态
        
        Args:
            symbol: 交易对名称
            order_id: 订单ID
            
        Returns:
            Dict: 订单状态
        """
        try:
            order = self._request(self.client.futures_get_order, symbol=symbol, orderId=order_id)
            return order
        except BinanceAPIException as e:
            logger.error(f"获取合约订单状态失败: {e}")
            return {}
    
    def place_market_order(self, symbol: str, side: str, quantity: float = None, notional: float = None) -> Dict:
        """
        下市价单
        
        Args:
            symbol: 交易对
            side: 方向 (BUY/SELL)
            quantity: 数量
            notional: 名义金额
            
        Returns:
            Dict: 订单信息
        """
        try:
            # 获取当前持仓模式
            position_mode = self._request(self.client.futures_get_position_mode)
            is_hedge_mode = position_mode['dualSidePosition']
            
            # 设置positionSide
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            if is_hedge_mode and symbol in ['BTCUSDT', 'ETHUSDT']:
                logger.info(f"对冲模式，设置positionSide: {position_side}")
            else:
                position_side = 'BOTH'
                logger.info("单向持仓模式，设置positionSide: BOTH")
            
            # 构建订单参数
            params = {
                'symbol': symbol,
                'side': side,
                'type': 'MARKET'
            }
            
            # 如果提供了notional，使用notional下单
            if notional:
                params['notional'] = notional
            # 否则使用quantity
            else:
                if not quantity:
                    raise ValueError("使用quantity时必须提供数量")
                params['quantity'] = quantity
            
            # 只在BTC和ETH的对冲模式下设置positionSide
            if is_hedge_mode and symbol in ['BTCUSDT', 'ETHUSDT']:
                params['positionSide'] = position_side
            
            logger.info(f"最终订单参数: {params}")
            return self._request(self.client.futures_create_order, **params)
            
        except Exception as e:
            logger.error(f"下合约单失败: {e}")
            raise

    def place_limit_order(self, symbol: str, side: str, quantity: float = None, price: float = None, notional: float = None) -> Dict:
        """
        下限价单
        
        Args:
            symbol: 交易对
            side: 方向 (BUY/SELL)
            quantity: 数量
            price: 价格
            notional: 名义金额
            
        Returns:
            Dict: 订单信息
        """
        try:
            # 获取当前持仓模式
            position_mode = self._request(self.client.futures_get_position_mode)
            is_hedge_mode = position_mode['dualSidePosition']
            logger.info(f"当前持仓模式: {'对冲模式' if is_hedge_mode else '单向持仓模式'}")
            
            # 对于非BTC/ETH交易对，确保使用单向持仓模式
            if symbol not in ['BTCUSDT', 'ETHUSDT'] and is_hedge_mode:
                logger.info(f"非BTC/ETH交易对，切换到单向持仓模式")
                self._request(self.client.futures_change_position_mode, dualSidePosition=False)
                logger.info("已切换到单向持仓模式")
            
            # 构建订单参数
            params = {
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'timeInForce': 'GTC'
            }
            
            # 获取当前市场价格
            current_price = self.get_current_price(symbol)
            if not current_price or current_price <= 0:
                raise ValueError(f"无法获取{symbol}当前价格或价格无效: {current_price}")
            
            logger.info(f"当前市场价格: {current_price}")
            
            # 验证价格是否在允许范围内
            if price:
                # 获取价格精度
                symbol_info = None
                for key, value in self.supported_symbols.items():
                    if value['symbol'] == symbol:
                        symbol_info = value
                        break
                
                if not symbol_info:
                    raise ValueError(f"不支持的交易对: {symbol}")
                
                # 格式化价格
                price = self.format_price(symbol, price)
                logger.info(f"格式化后的价格: {price}")
                
                # 检查价格是否在允许范围内
                if side == 'BUY':
                    if price > current_price * 1.1:  # 买单价格不能高于当前价格的110%
                        raise ValueError(f"买单价格 {price} 不能高于当前价格 {current_price} 的110%")
                else:  # SELL
                    if price < current_price * 0.9:  # 卖单价格不能低于当前价格的90%
                        raise ValueError(f"卖单价格 {price} 不能低于当前价格 {current_price} 的90%")
            else:
                # 如果没有提供价格，使用当前市场价格
                price = current_price
                logger.info(f"未提供价格，使用当前市场价格: {price}")
            
            params['price'] = price
            
            # 如果提供了notional，使用notional计算quantity
            if notional:
                logger.info(f"使用notional下单: notional={notional}, price={price}, current_price={current_price}")
                
                # 计算数量
                quantity = notional / price
                if not quantity or quantity <= 0:
                    raise ValueError(f"计算出的数量无效: {quantity}")
                    
                # 格式化数量
                quantity = self.format_quantity(symbol, quantity)
                if not quantity or quantity <= 0:
                    raise ValueError(f"格式化后的数量无效: {quantity}")
                
                logger.info(f"使用价格 {price} 计算数量: {quantity}")
                
                # 设置参数
                params['quantity'] = quantity
                
                # 计算实际保证金
                actual_margin = notional / self.trading_config['leverage']
                logger.info(f"名义金额: {notional} USDT, 实际保证金: {actual_margin} USDT")
            else:
                # 使用quantity和price
                if not quantity:
                    raise ValueError("使用quantity时必须提供数量")
                if quantity <= 0:
                    raise ValueError(f"数量必须大于0: {quantity}")
                    
                # 格式化数量
                quantity = self.format_quantity(symbol, quantity)
                if not quantity or quantity <= 0:
                    raise ValueError(f"格式化后的数量无效: {quantity}")
                
                params['quantity'] = quantity
            
            logger.info(f"最终订单参数: {params}")
            return self._request(self.client.futures_create_order, **params)
            
        except Exception as e:
            logger.error(f"下合约单失败: {e}")
            raise

    def place_stop_loss_order(self, symbol: str, side: str, quantity: float, stop_price: float) -> Dict:
        """
        下止损单
        
        Args:
            symbol: 交易对
            side: 方向 (BUY/SELL)
            quantity: 数量
            stop_price: 止损价格
            
        Returns:
            Dict: 订单信息
        """
        try:
            # 获取当前持仓模式
            position_mode = self._request(self.client.futures_get_position_mode)
            is_hedge_mode = position_mode['dualSidePosition']
            
            # 设置positionSide
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            if is_hedge_mode:
                logger.info(f"对冲模式，设置positionSide: {position_side}")
            else:
                position_side = 'BOTH'
                logger.info("单向持仓模式，设置positionSide: BOTH")
            
            # 使用STOP_MARKET替代STOP_LIMIT
            params = {
                'symbol': symbol,
                'side': side,
                'type': 'STOP_MARKET',
                'stopPrice': stop_price,
                'workingType': 'MARK_PRICE',
                'quantity': quantity,
                'timeInForce': 'GTC',
                'reduceOnly': True
            }
            
            if is_hedge_mode:
                params['positionSide'] = position_side
            
            logger.info(f"最终订单参数: {params}")
            return self._request(self.client.futures_create_order, **params)
            
        except Exception as e:
            logger.error(f"下止损单失败: {e}")
            raise

    def place_take_profit_order(self, symbol: str, side: str, quantity: float, stop_price: float) -> Dict:
        """
        下合约止盈单
        
        Args:
            symbol: 交易对符号
            side: 方向 (BUY/SELL)
            quantity: 数量
            stop_price: 触发价格
            
        Returns:
            Dict: 订单信息
        """
        return self.place_order(symbol, side, 'TAKE_PROFIT_LIMIT', quantity, stop_price=stop_price, price=stop_price)
    
    def update_trading_config(self, config: Dict):
        """
        更新交易配置
        
        Args:
            config: 新的配置字典
        """
        self.trading_config.update(config)
        logger.info(f"交易配置已更新: {config}")
    
    def get_trading_config(self) -> Dict:
        """
        获取当前交易配置
        
        Returns:
            Dict: 交易配置
        """
        return self.trading_config
    
    def get_active_orders(self) -> Dict:
        """
        获取当前活跃订单
        
        Returns:
            Dict: 活跃订单字典
        """
        return self.active_orders
    
    def close_all_positions(self, symbol: str = None):
        """
        平掉所有合约仓位
        
        Args:
            symbol: 交易对名称，如果为None则平掉所有交易对的仓位
        """
        try:
            # 获取所有未完成订单
            open_orders = self.get_open_orders(symbol)
            
            # 取消所有未完成订单
            for order in open_orders:
                self.cancel_order(order['symbol'], order['orderId'])
            
            # 获取账户信息
            account = self.get_account_info()
            
            # 遍历所有持仓
            for position in account['positions']:
                if float(position['positionAmt']) != 0:
                    # 构建平仓参数
                    close_params = {
                        'symbol': position['symbol'],
                        'side': 'SELL' if float(position['positionAmt']) > 0 else 'BUY',
                        'type': 'MARKET',
                        'quantity': abs(float(position['positionAmt'])),
                        'reduceOnly': True
                    }
                    
                    # 平仓
                    self._request(self.client.futures_create_order, **close_params)
            
            logger.info("所有合约仓位已平仓")
            
        except BinanceAPIException as e:
            logger.error(f"平仓失败: {e}")

    def read_trading_signals(self) -> List[Dict]:
        """
        读取交易信号，只读取最近4小时的信号
        """
        try:
            # 获取最新的分析结果文件
            analysis_dir = os.path.join('C:\\', 'Users', 'Administrator', 'Desktop', 'Discord', 'data', 'analysis_results')
            if not os.path.exists(analysis_dir):
                logger.warning(f"分析结果目录不存在: {analysis_dir}")
                return []
                
            files = [f for f in os.listdir(analysis_dir) if f.endswith('.csv')]
            if not files:
                logger.warning(f"未找到分析结果文件: {analysis_dir}")
                return []
                
            latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(analysis_dir, x)))
            file_path = os.path.join(analysis_dir, latest_file)
            logger.info(f"读取最新的分析结果文件: {file_path}")
            
            # 读取CSV文件
            df = pd.read_csv(file_path)
            
            # 过滤出有效的分析结果
            df = df[df['analysis.分析失败'].isna()]
            
            # 获取当前时间
            current_time = pd.Timestamp.now()
            
            signals = []
            for _, row in df.iterrows():
                try:
                    # 检查信号时间是否在最近4小时内
                    signal_time = pd.to_datetime(row['timestamp'])
                    if (current_time - signal_time).total_seconds() > 4 * 3600:  # 4小时 = 4 * 3600秒
                        continue
                        
                    # 检查交易币种
                    symbol = row['analysis.交易币种']
                    if pd.isna(symbol):
                        continue
                        
                    # 检查是否支持该交易对
                    if not self.is_symbol_supported(symbol):
                        logger.warning(f"不支持的交易对: {symbol}")
                        continue
                        
                    # 检查方向
                    direction = row['analysis.方向']
                    if pd.isna(direction):
                        continue
                        
                    # 转换方向
                    side = 'BUY' if direction == '做多' else 'SELL' if direction == '做空' else None
                    if not side:
                        continue
                        
                    # 检查入场价格
                    entry_price = row['analysis.入场点位1']
                    if pd.isna(entry_price):
                        continue
                    try:
                        entry_price = float(entry_price)
                    except (ValueError, TypeError):
                        logger.warning(f"无效的入场价格: {entry_price}")
                        continue
                        
                    # 检查止损价格
                    stop_loss = row['analysis.止损点位1']
                    if pd.isna(stop_loss):
                        continue
                    try:
                        stop_loss = float(stop_loss)
                    except (ValueError, TypeError):
                        logger.warning(f"无效的止损价格: {stop_loss}")
                        continue
                        
                    # 检查止盈价格
                    target_price = None
                    for i in range(1, 4):
                        target_col = f'analysis.止盈点位{i}'
                        if target_col in row and not pd.isna(row[target_col]):
                            try:
                                target_price = float(row[target_col])
                                break
                            except (ValueError, TypeError):
                                logger.warning(f"无效的止盈价格: {row[target_col]}")
                                continue
                    
                    # 检查频道
                    channel = row['channel']
                    if pd.isna(channel):
                        continue
                        
                    # 如果是BTC，检查是否在配置中
                    if symbol == 'BTCUSDT':
                        if channel not in self.btc_channel_positions:
                            continue
                            
                    # 创建信号
                    signal = {
                        'symbol': f"{symbol}USDT",
                        'side': side,
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'channel': channel,
                        'timestamp': row['timestamp']
                    }
                    
                    signals.append(signal)
                    logger.info(f"添加交易信号: {signal}")
                    
                except Exception as e:
                    logger.error(f"处理信号时出错: {e}, 行数据: {row.to_dict()}")
                    continue
                    
            return signals
            
        except Exception as e:
            logger.error(f"读取交易信号时出错: {e}")
            return []

    def check_balance_sufficient(self, symbol: str, notional: float) -> bool:
        """
        检查账户余额是否足够开仓
        
        Args:
            symbol: 交易对
            notional: 名义金额
            
        Returns:
            bool: 余额是否足够
        """
        try:
            # 获取账户信息
            account = self._request(self.client.futures_account)
            
            # 获取可用余额
            available_balance = 0
            for asset in account['assets']:
                if asset['asset'] == 'USDT':
                    available_balance = float(asset['availableBalance'])
                    break
            
            # 计算所需保证金
            required_margin = notional / self.trading_config['leverage']
            
            logger.info(f"账户信息:")
            logger.info(f"  可用余额: {available_balance:.2f} USDT")
            logger.info(f"  开仓所需保证金: {required_margin:.2f} USDT")
            
            # 检查是否有足够的保证金
            if available_balance < required_margin:
                logger.error(f"余额不足: 需要 {required_margin:.2f} USDT，当前可用 {available_balance:.2f} USDT")
                return False
                
            return True
            
        except BinanceAPIException as e:
            logger.error(f"检查余额失败: {e}")
            return False

    def get_signal_key(self, signal: Dict) -> str:
        """
        生成信号唯一标识
        """
        try:
            symbol = signal.get('symbol', '')
            side = signal.get('side', '')
            entry_price = signal.get('entry_price', 0)
            stop_loss = signal.get('stop_loss', 0)
            target_price = signal.get('target_price', 0)
            channel = signal.get('channel', '')
            timestamp = signal.get('timestamp', '')
            
            # 生成唯一标识
            key = f"{symbol}_{side}_{entry_price}_{stop_loss}_{target_price}_{channel}_{timestamp}"
            return key
            
        except Exception as e:
            logger.error(f"生成信号标识时出错: {e}")
            return ""

    def is_signal_executed(self, signal: Dict) -> bool:
        """
        检查交易信号是否已执行
        
        Args:
            signal: 交易信号字典
            
        Returns:
            bool: 是否已执行
        """
        signal_key = self.get_signal_key(signal)
        current_time = time.time()
        
        # 检查是否有相同入场价格的订单在4小时内执行过
        if signal_key in self.executed_signals:
            last_execution_time = self.executed_signals[signal_key]
            if current_time - last_execution_time < 4 * 3600:  # 4小时 = 4 * 3600秒
                logger.info(f"信号 {signal_key} 在4小时内已执行过，跳过")
                return True
        
        # 检查是否有相同特征的订单在4小时内执行过（忽略时间戳）
        base_key = '_'.join(signal_key.split('_')[:-1])  # 移除时间戳部分
        for key in self.executed_signals.keys():
            if key.startswith(base_key):
                last_execution_time = self.executed_signals[key]
                if current_time - last_execution_time < 4 * 3600:
                    logger.info(f"发现相似信号 {key} 在4小时内已执行过，跳过")
                    return True
        
        return False

    def mark_signal_executed(self, signal: Dict):
        """
        标记交易信号为已执行
        
        Args:
            signal: 交易信号字典
        """
        signal_key = self.get_signal_key(signal)
        current_time = time.time()
        
        # 更新执行时间
        self.executed_signals[signal_key] = current_time
        
        # 保存到文件
        self.save_executed_signals()
        logger.info(f"标记信号为已执行: {signal_key}")

    def check_existing_orders(self, symbol: str, side: str, entry_price: float) -> bool:
        """
        检查是否存在相同信号的挂单
        
        Args:
            symbol: 交易对
            side: 交易方向
            entry_price: 入场价格
            
        Returns:
            bool: 是否存在相同信号的挂单
        """
        try:
            # 获取所有未完成订单
            open_orders = self.get_open_orders(symbol)
            
            # 检查是否有相同信号的挂单
            for order in open_orders:
                # 检查是否是限价单
                if order['type'] == 'LIMIT':
                    # 检查方向是否相同
                    if order['side'] == side:
                        # 检查价格是否接近（允许0.1%的误差）
                        order_price = float(order['price'])
                        price_diff = abs(order_price - entry_price) / entry_price
                        if price_diff <= 0.001:  # 0.1%的误差
                            logger.info(f"发现相同信号的挂单: {order}")
                            return True
            
            # 检查订单配对关系中是否有相同信号的活跃订单
            for pair in self.order_pairs.values():
                if pair['status'] == 'active':
                    if (pair['symbol'] == symbol and 
                        pair['side'] == side and 
                        abs(float(pair['entry_price']) - entry_price) / entry_price <= 0.001):
                        logger.info(f"发现相同信号的活跃订单: {pair}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"检查现有挂单时出错: {e}")
            return False

    def execute_trading_signals(self, signals: List[Dict]):
        """
        执行交易信号
        """
        if not signals:
            logger.info("没有新的交易信号需要执行")
            return
            
        logger.info(f"发现 {len(signals)} 个交易信号")
        
        for signal in signals:
            try:
                # 验证信号
                if not self.validate_signal(signal):
                    logger.warning(f"信号验证失败: {signal}")
                    continue
                    
                # 获取信号参数
                symbol = signal.get('symbol')
                side = signal.get('side')
                entry_price = signal.get('entry_price')
                stop_loss = signal.get('stop_loss')
                target_price = signal.get('target_price')
                channel = signal.get('channel')
                
                # 打印所有参数值
                logger.info(f"信号参数详情:")
                logger.info(f"  symbol: {symbol}")
                logger.info(f"  side: {side}")
                logger.info(f"  entry_price: {entry_price}")
                logger.info(f"  stop_loss: {stop_loss}")
                logger.info(f"  target_price: {target_price}")
                logger.info(f"  channel: {channel}")
                
                # 检查必要参数
                if not all([symbol, side, entry_price]):
                    logger.warning(f"信号参数不完整: symbol={symbol}, side={side}, entry_price={entry_price}")
                    continue
                
                # 检查是否有止损或止盈
                if not stop_loss and not target_price:
                    logger.warning(f"信号缺少止损和止盈价格")
                    continue
                
                # 生成信号标识
                signal_key = self.get_signal_key(signal)
                logger.info(f"生成信号标识: {signal_key}")
                
                # 检查是否已执行
                if self.is_signal_executed(signal):
                    logger.info(f"信号已执行，跳过: {symbol} {side}")
                    continue
                
                # 检查是否有相同方向的未完成订单
                if self.check_existing_orders(symbol, side, entry_price):
                    logger.info(f"存在相同方向的未完成订单，跳过: {symbol} {side}")
                    continue
                
                # 获取当前市场价格
                current_price = self.get_current_price(symbol)
                logger.info(f"当前市场价格: {current_price}")
                
                # 获取账户信息
                account_info = self.get_account_info()
                available_balance = float(account_info.get('availableBalance', 0))
                logger.info(f"账户信息:\n  可用余额: {available_balance:.2f} USDT")
                
                # 计算开仓数量
                if symbol == 'BTCUSDT':
                    # BTC使用频道配置
                    if not channel:
                        logger.warning(f"BTC交易缺少频道信息: {signal}")
                        continue
                        
                    # 获取BTC仓位大小
                    position_size = self.get_btc_position_size(channel)
                    if not position_size:
                        logger.warning(f"无法获取BTC仓位大小，跳过")
                        continue
                        
                    # 计算最大仓位价值
                    max_position_value = self.get_btc_max_position_value(channel)
                    if not max_position_value:
                        logger.warning(f"无法获取BTC最大仓位价值，跳过")
                        continue
                        
                    # 检查余额是否足够
                    if not self.check_balance_sufficient(symbol, max_position_value):
                        logger.warning(f"余额不足，跳过BTC交易")
                        continue
                        
                    # 设置名义金额
                    notional = max_position_value
                    logger.info(f"使用最大仓位价值: {notional} USDT")
                    logger.info(f"开仓所需保证金: {position_size} USDT")
                else:
                    # ETH和其他币种使用固定配置
                    # 设置名义金额为1000U
                    notional = 1000
                    # 使用20倍杠杆
                    leverage = 20
                    # 计算实际开仓金额（保证金）
                    actual_position = notional / leverage
                    
                    # 检查余额是否足够
                    if not self.check_balance_sufficient(symbol, notional):
                        logger.warning(f"余额不足，跳过{symbol}交易")
                        continue
                        
                    logger.info(f"使用固定配置: 名义金额={notional}U, 实际开仓={actual_position}U, 杠杆={leverage}倍")
                    logger.info(f"开仓所需保证金: {actual_position} USDT")
                    
                    # 执行开仓
                    try:
                        # 所有币种都使用限价单
                        order = self.place_limit_order(symbol, side, notional=notional, price=entry_price)
                        
                        if order:
                            logger.info(f"下入场单成功: {symbol} {side}")
                            
                            # 设置止损单（如果有止损价）
                            if stop_loss:
                                try:
                                    stop_loss_order = self.place_stop_loss_order(symbol, side, notional=notional, stop_price=stop_loss)
                                    if stop_loss_order:
                                        logger.info(f"下止损单成功: {symbol} {side} @ {stop_loss}")
                                except Exception as e:
                                    logger.error(f"下止损单失败: {e}")
                                    logger.error(f"下止损单失败: {symbol} {side} @ {stop_loss}")
                            
                            # 设置止盈单（如果有目标价）
                            if target_price:
                                try:
                                    take_profit_order = self.place_take_profit_order(symbol, side, notional=notional, stop_price=target_price)
                                    if take_profit_order:
                                        logger.info(f"下止盈单成功: {symbol} {side} @ {target_price}")
                                except Exception as e:
                                    logger.error(f"下止盈单失败: {e}")
                                    logger.error(f"下止盈单失败: {symbol} {side} @ {target_price}")
                            
                            # 标记信号为已执行
                            self.mark_signal_executed(signal)
                            logger.info(f"已保存 {len(self.executed_signals)} 条已执行订单记录")
                            logger.info(f"标记信号为已执行: {signal_key}")
                        else:
                            logger.error(f"下入场单失败: {symbol} {side}")
                            # 即使入场单失败，也标记信号为已执行
                            self.mark_signal_executed(signal)
                            logger.info(f"入场单失败，标记信号为已执行: {signal_key}")
                            
                    except Exception as e:
                        logger.error(f"执行交易信号时出错: {e}, 信号: {signal}")
                        # 即使发生异常，也标记信号为已执行
                        self.mark_signal_executed(signal)
                        logger.info(f"执行出错，标记信号为已执行: {signal_key}")
                        continue
                    
            except Exception as e:
                logger.error(f"处理交易信号时出错: {e}, 信号: {signal}")
                continue

    def check_order_status(self):
        """
        检查所有订单的状态，更新订单配对关系
        """
        try:
            for entry_order_id, pair in list(self.order_pairs.items()):
                if pair['status'] != 'active':
                    continue
                
                try:
                    # 检查入场单状态
                    entry_order = self.get_order_status(pair['symbol'], int(entry_order_id))
                    if not entry_order:
                        continue
                    
                    # 如果入场单已成交
                    if entry_order['status'] == 'FILLED':
                        # 检查止损单状态
                        if pair['stop_loss_order_id']:
                            stop_loss_order = self.get_order_status(pair['symbol'], pair['stop_loss_order_id'])
                            if stop_loss_order and stop_loss_order['status'] == 'FILLED':
                                pair['status'] = 'closed_by_stop_loss'
                                logger.info(f"订单 {entry_order_id} 已通过止损平仓")
                        
                        # 检查止盈单状态
                        if pair['take_profit_order_id']:
                            take_profit_order = self.get_order_status(pair['symbol'], pair['take_profit_order_id'])
                            if take_profit_order and take_profit_order['status'] == 'FILLED':
                                pair['status'] = 'closed_by_take_profit'
                                logger.info(f"订单 {entry_order_id} 已通过止盈平仓")
                    
                    # 如果入场单已取消
                    elif entry_order['status'] == 'CANCELED':
                        # 取消对应的止损止盈单
                        if pair['stop_loss_order_id']:
                            try:
                                self.cancel_order(pair['symbol'], pair['stop_loss_order_id'])
                            except:
                                pass
                        if pair['take_profit_order_id']:
                            try:
                                self.cancel_order(pair['symbol'], pair['take_profit_order_id'])
                            except:
                                pass
                        pair['status'] = 'canceled'
                        logger.info(f"订单 {entry_order_id} 已取消")
                    
                except Exception as e:
                    logger.error(f"检查订单 {entry_order_id} 状态时出错: {e}")
                    continue
            
            # 保存更新后的订单配对关系
            self.save_order_pairs()
            
        except Exception as e:
            logger.error(f"检查订单状态时出错: {e}")

    def monitor_and_trade(self, interval: int = 60):
        """
        监控并执行交易
        
        Args:
            interval: 检查间隔（秒）
        """
        logger.info("开始监控交易信号...")
        last_cleanup_time = time.time()
        
        while True:
            try:
                current_time = time.time()
                
                # 每4小时清理一次过期记录
                if current_time - last_cleanup_time >= 4 * 3600:  # 4小时
                    self.clean_expired_signals()
                    last_cleanup_time = current_time
                    logger.info("已清理过期记录")
                
                # 检查订单状态
                self.check_order_status()
                
                # 读取交易信号
                signals = self.read_trading_signals()
                if signals:
                    logger.info(f"发现 {len(signals)} 个交易信号")
                    
                    # 过滤掉已执行的信号
                    new_signals = []
                    for signal in signals:
                        try:
                            # 检查信号是否已执行
                            if self.is_signal_executed(signal):
                                logger.info(f"跳过已执行的信号: {self.get_signal_key(signal)}")
                                continue
                            
                            # 检查是否有相同信号的挂单
                            if self.check_existing_orders(signal['symbol'], signal['side'], signal['entry_price']):
                                logger.info(f"已存在相同信号的挂单，跳过: {signal['symbol']} {signal['side']} @ {signal['entry_price']}")
                                continue
                            
                            # 验证信号的有效性
                            if not self.validate_signal(signal):
                                logger.warning(f"信号验证失败: {signal}")
                                continue
                            
                            new_signals.append(signal)
                            logger.info(f"添加新信号到处理队列: {self.get_signal_key(signal)}")
                            
                        except Exception as e:
                            logger.error(f"处理信号时出错: {e}")
                            continue
                    
                    if new_signals:
                        logger.info(f"执行 {len(new_signals)} 个新交易信号")
                        self.execute_trading_signals(new_signals)
                    else:
                        logger.info("没有新的交易信号需要执行")
                
                # 等待下一次检查
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"监控交易时出错: {e}")
                time.sleep(interval)

    def validate_signal(self, signal: Dict) -> bool:
        """
        验证交易信号
        """
        try:
            logger.info(f"开始验证信号: {signal}")
            
            # 检查必要参数
            required_fields = ['symbol', 'side', 'entry_price']
            for field in required_fields:
                if field not in signal or signal[field] is None:
                    logger.warning(f"信号缺少必要字段: {field}")
                    return False
                    
            symbol = signal['symbol']
            side = signal['side']
            entry_price = float(signal['entry_price'])
            stop_loss = float(signal['stop_loss']) if signal.get('stop_loss') else None
            target_price = float(signal['target_price']) if signal.get('target_price') else None
            channel = signal.get('channel')
            
            # 检查是否有止损或止盈
            if not stop_loss and not target_price:
                logger.warning(f"信号缺少止损和止盈价格")
                return False
            
            logger.info(f"信号参数解析:")
            logger.info(f"  symbol: {symbol}")
            logger.info(f"  side: {side}")
            logger.info(f"  entry_price: {entry_price}")
            logger.info(f"  stop_loss: {stop_loss}")
            logger.info(f"  target_price: {target_price}")
            logger.info(f"  channel: {channel}")
            
            # 获取当前市场价格
            current_price = self.get_current_price(symbol)
            logger.info(f"当前市场价格: {current_price}")
            
            # 获取账户信息
            account_info = self.get_account_info()
            available_balance = float(account_info.get('availableBalance', 0))
            logger.info(f"账户信息:\n  可用余额: {available_balance:.2f} USDT")
            
            # 验证价格关系
            if side == 'BUY':
                if stop_loss and entry_price <= stop_loss:
                    logger.warning(f"买入信号价格关系错误: 入场价({entry_price}) <= 止损价({stop_loss})")
                    return False
                if target_price and target_price <= entry_price:
                    logger.warning(f"买入信号价格关系错误: 目标价({target_price}) <= 入场价({entry_price})")
                    return False
            else:  # SELL
                if stop_loss and entry_price >= stop_loss:
                    logger.warning(f"卖出信号价格关系错误: 入场价({entry_price}) >= 止损价({stop_loss})")
                    return False
                if target_price and target_price >= entry_price:
                    logger.warning(f"卖出信号价格关系错误: 目标价({target_price}) >= 入场价({entry_price})")
                    return False
            
            # 检查交易对是否支持
            if not self.is_symbol_supported(symbol):
                logger.warning(f"不支持的交易对: {symbol}")
                return False
            
            # 检查余额是否足够
            if symbol == 'BTCUSDT':
                # BTC使用频道配置
                if not channel:
                    logger.warning(f"BTC交易缺少频道信息")
                    return False
                    
                # 获取BTC仓位大小
                position_size = self.get_btc_position_size(channel)
                if not position_size:
                    logger.warning(f"无法获取BTC仓位大小")
                    return False
                    
                # 计算最大仓位价值
                max_position_value = self.get_btc_max_position_value(channel)
                if not max_position_value:
                    logger.warning(f"无法获取BTC最大仓位价值")
                    return False
                    
                # 检查余额是否足够
                if not self.check_balance_sufficient(symbol, max_position_value):
                    logger.warning(f"余额不足，需要 {max_position_value} USDT")
                    return False
            else:
                # ETH和其他币种使用固定配置
                # 设置名义金额为1000U
                notional = 1000
                # 使用20倍杠杆
                leverage = 20
                # 计算实际开仓金额（保证金）
                actual_position = notional / leverage
                
                # 检查余额是否足够
                if not self.check_balance_sufficient(symbol, notional):
                    logger.warning(f"余额不足，需要 {notional} USDT")
                    return False
                    
                logger.info(f"使用固定配置: 名义金额={notional}U, 实际开仓={actual_position}U, 杠杆={leverage}倍")
            
            logger.info(f"信号验证通过: {symbol} {side}")
            return True
            
        except Exception as e:
            logger.error(f"验证信号时出错: {e}")
            return False

    def get_cross_margin_account(self) -> Dict:
        """
        获取全仓账户信息
        """
        try:
            response = self._request('GET', '/sapi/v1/margin/account')
            if not response:
                return {}
                
            # 只记录总资产信息
            total_net_asset = float(response.get('totalNetAssetOfBtc', 0))
            total_asset = float(response.get('totalAssetOfBtc', 0))
            total_liability = float(response.get('totalLiabilityOfBtc', 0))
            
            logger.info(f"全仓账户总资产: {total_asset:.8f} BTC")
            logger.info(f"全仓账户总负债: {total_liability:.8f} BTC")
            logger.info(f"全仓账户净资产: {total_net_asset:.8f} BTC")
            
            return response
            
        except Exception as e:
            logger.error(f"获取全仓账户信息时出错: {e}")
            return {}

    def get_position_info(self) -> Dict:
        """
        获取当前持仓信息
        
        Returns:
            Dict: 持仓信息
        """
        try:
            positions = self._request(self.client.futures_position_information)
            total_position_value = 0
            has_position = False
            
            logger.info("\n当前持仓信息:")
            for position in positions:
                position_amt = float(position['positionAmt'])
                if position_amt != 0:  # 只显示有持仓的
                    has_position = True
                    entry_price = float(position['entryPrice'])
                    mark_price = float(position['markPrice'])
                    position_value = abs(position_amt * mark_price)
                    total_position_value += position_value
                    
                    logger.info(f"交易对: {position['symbol']}")
                    logger.info(f"  持仓方向: {'多' if position_amt > 0 else '空'}")
                    logger.info(f"  持仓数量: {abs(position_amt)}")
                    logger.info(f"  入场价格: {entry_price}")
                    logger.info(f"  标记价格: {mark_price}")
                    logger.info(f"  持仓价值: {position_value:.2f} USDT")
                    logger.info(f"  未实现盈亏: {float(position['unRealizedProfit']):.2f} USDT")
                    logger.info("-------------------")
            
            if has_position:
                logger.info(f"\n总持仓价值: {total_position_value:.2f} USDT")
            else:
                logger.info("当前没有持仓")
                
            return positions
            
        except BinanceAPIException as e:
            logger.error(f"获取持仓信息失败: {e}")
            return {}

    def get_server_time(self) -> int:
        """
        获取币安服务器时间
        
        Returns:
            int: 服务器时间戳（毫秒）
        """
        try:
            server_time = self._request(self.client.get_server_time)
            return server_time['serverTime']
        except Exception as e:
            logger.error(f"获取服务器时间失败: {e}")
            return int(time.time() * 1000)  # 如果失败则返回本地时间

    def get_timestamp(self) -> int:
        """
        获取当前时间戳，考虑服务器时间差
        
        Returns:
            int: 调整后的时间戳（毫秒）
        """
        return int(time.time() * 1000) + self.time_offset

    def get_all_supported_symbols(self) -> Dict:
        """
        获取所有支持的USDT合约交易对信息
        
        Returns:
            Dict: 交易对信息字典
        """
        try:
            # 获取所有合约交易对信息
            exchange_info = self._request(self.client.futures_exchange_info)
            supported_symbols = {}
            
            # 处理从API获取的交易对
            for symbol_info in exchange_info['symbols']:
                # 只处理USDT合约
                if symbol_info['quoteAsset'] == 'USDT' and symbol_info['status'] == 'TRADING':
                    symbol = symbol_info['symbol']
                    base_asset = symbol_info['baseAsset']
                    
                    # 特殊处理BTC和ETH
                    if base_asset == 'BTC':
                        symbol = 'BTCUSDT'
                    elif base_asset == 'ETH':
                        symbol = 'ETHUSDT'
                    
                    # 获取数量精度
                    quantity_precision = 0
                    min_qty = 0.001  # 默认值
                    for filter in symbol_info['filters']:
                        if filter['filterType'] == 'LOT_SIZE':
                            step_size = float(filter['stepSize'])
                            quantity_precision = len(str(step_size).rstrip('0').split('.')[-1]) if '.' in str(step_size) else 0
                            min_qty = float(filter['minQty'])
                            break
                    
                    # 获取价格精度
                    price_precision = 0
                    for filter in symbol_info['filters']:
                        if filter['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter['tickSize'])
                            price_precision = len(str(tick_size).rstrip('0').split('.')[-1]) if '.' in str(tick_size) else 0
                            break
                    
                    # 获取最小名义金额
                    min_notional = 5  # 默认值
                    for filter in symbol_info['filters']:
                        if filter['filterType'] == 'MIN_NOTIONAL':
                            min_notional = float(filter['notional'])
                            break
                    
                    supported_symbols[base_asset] = {
                        'symbol': symbol,
                        'quantity_precision': quantity_precision,
                        'price_precision': price_precision,
                        'min_qty': min_qty,
                        'min_notional': min_notional
                    }
            
            # 确保全局变量被更新
            global SUPPORTED_SYMBOLS
            SUPPORTED_SYMBOLS = supported_symbols
            
            logger.info(f"已加载 {len(supported_symbols)} 个USDT合约交易对")
            return supported_symbols
            
        except Exception as e:
            logger.error(f"获取支持的交易对信息失败: {e}")
            # 如果API调用失败，返回空字典
            return {}

    def _request(self, method, *args, **kwargs):
        """
        发送请求到币安API，自动处理时间同步
        
        Args:
            method: API方法
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            请求结果
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 更新请求时间戳
                if 'timestamp' in kwargs:
                    kwargs['timestamp'] = self.get_timestamp()
                return method(*args, **kwargs)
            except BinanceAPIException as e:
                if e.code == -1021:  # 时间戳错误
                    retry_count += 1
                    if retry_count < max_retries:
                        # 重新同步时间
                        try:
                            server_time = self.client.get_server_time()
                            local_time = int(time.time() * 1000)
                            self.time_offset = server_time['serverTime'] - local_time
                            logger.info(f"重新同步服务器时间，新的时间差: {self.time_offset}ms")
                            time.sleep(0.5)  # 等待0.5秒后重试
                            continue
                        except Exception as sync_error:
                            logger.error(f"重新同步时间失败: {sync_error}")
                    else:
                        logger.error(f"重试{max_retries}次后仍然失败")
                raise
            except Exception as e:
                logger.error(f"请求失败: {e}")
                raise

    def clean_expired_signals(self):
        """
        清理过期的执行记录（超过4小时的记录）
        """
        try:
            current_time = time.time()
            expired_keys = []
            
            # 找出过期的记录
            for signal_key, execution_time in self.executed_signals.items():
                # 检查是否超过4小时
                if current_time - execution_time >= 4 * 3600:  # 4小时 = 4 * 3600秒
                    # 检查信号是否已经完成（通过订单配对关系）
                    signal_parts = signal_key.split('_')
                    if len(signal_parts) >= 4:
                        symbol = signal_parts[0]
                        side = signal_parts[1]
                        entry_price = float(signal_parts[2])
                        
                        # 检查是否有对应的已完成订单
                        has_completed_order = False
                        for pair in self.order_pairs.values():
                            if (pair['symbol'] == symbol and 
                                pair['side'] == side and 
                                abs(float(pair['entry_price']) - entry_price) / entry_price <= 0.001):
                                if pair['status'] in ['closed_by_stop_loss', 'closed_by_take_profit']:
                                    has_completed_order = True
                                    break
                        
                        # 如果没有已完成订单，则保留记录
                        if not has_completed_order:
                            continue
                    
                    expired_keys.append(signal_key)
            
            # 删除过期记录
            for key in expired_keys:
                del self.executed_signals[key]
            
            if expired_keys:
                logger.info(f"已清理 {len(expired_keys)} 条过期记录")
                # 保存更新后的记录
                self.save_executed_signals()
                
        except Exception as e:
            logger.error(f"清理过期记录时出错: {e}")

    def load_btc_position_config(self) -> Dict:
        """
        加载BTC仓位配置文件（Excel格式）
        """
        try:
            config_path = os.path.join('C:\\', 'Users', 'Administrator', 'Desktop', 'btc仓位.xlsx')
            if not os.path.exists(config_path):
                logger.warning(f"BTC仓位配置文件不存在: {config_path}")
                return {}
                
            # 读取Excel文件
            try:
                df = pd.read_excel(config_path)
                logger.info(f"成功读取BTC仓位配置文件: {config_path}")
                logger.info(f"Excel文件列名: {df.columns.tolist()}")
                
                # 检查必要的列是否存在
                required_columns = ['频道', '比例']  # 可能的列名
                found_columns = []
                
                for col in df.columns:
                    if '频道' in col:
                        channel_col = col
                        found_columns.append(col)
                    elif '比例' in col:
                        ratio_col = col
                        found_columns.append(col)
                        
                if len(found_columns) < 2:
                    logger.error(f"Excel文件缺少必要的列，当前列名: {df.columns.tolist()}")
                    return {}
                    
                logger.info(f"使用列名: 频道={channel_col}, 比例={ratio_col}")
                
                # 将DataFrame转换为字典格式
                config = {}
                for _, row in df.iterrows():
                    channel = str(row[channel_col]).strip()
                    ratio = float(row[ratio_col])
                    if channel and not pd.isna(ratio):
                        config[channel] = {'ratio': ratio}
                        
                logger.info(f"已加载BTC仓位配置: {config}")
                return config
                
            except Exception as e:
                logger.error(f"读取Excel文件时出错: {str(e)}")
                return {}
                
        except Exception as e:
            logger.error(f"加载BTC仓位配置时出错: {str(e)}")
            return {}

    def get_btc_position_size(self, channel: str) -> Optional[float]:
        """
        获取BTC仓位大小
        
        Args:
            channel: 频道名称
            
        Returns:
            float: 仓位大小（USDT）
        """
        try:
            # 获取频道配置
            channel_config = self.btc_channel_positions.get(channel)
            if not channel_config:
                logger.warning(f"未找到频道配置: {channel}")
                return None
                
            # 获取仓位比例
            position_ratio = float(channel_config.get('ratio', 0))
            if position_ratio <= 0:
                logger.warning(f"无效的仓位比例: {position_ratio}")
                return None
                
            # 计算仓位大小
            position_size = self.btc_initial_capital * position_ratio
            logger.info(f"频道 {channel} 仓位大小: {position_size} USDT")
            return position_size
            
        except Exception as e:
            logger.error(f"获取BTC仓位大小失败: {e}")
            return None
            
    def get_btc_max_position_value(self, channel: str) -> Optional[float]:
        """
        获取BTC最大仓位价值
        
        Args:
            channel: 频道名称
            
        Returns:
            float: 最大仓位价值（USDT）
        """
        try:
            # 获取仓位大小
            position_size = self.get_btc_position_size(channel)
            if not position_size:
                return None
                
            # 计算最大仓位价值
            max_position_value = position_size * self.btc_leverage
            logger.info(f"频道 {channel} 最大仓位价值: {max_position_value} USDT")
            return max_position_value
            
        except Exception as e:
            logger.error(f"获取BTC最大仓位价值失败: {e}")
            return None

    def update_btc_position_config(self, new_config: Dict) -> bool:
        """
        更新BTC仓位配置
        
        Args:
            new_config: 新的仓位配置
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 验证配置总和是否为1
            total_ratio = sum(new_config.values())
            if abs(total_ratio - 1.0) > 0.0001:  # 允许0.01%的误差
                logger.error(f"BTC仓位配置总和必须为1，当前总和: {total_ratio}")
                return False
                
            # 更新配置
            self.btc_channel_positions = new_config
            self.save_btc_position_config(new_config)
            logger.info(f"已更新BTC仓位配置: {new_config}")
            return True
            
        except Exception as e:
            logger.error(f"更新BTC仓位配置失败: {e}")
            return False

    def get_all_btc_channel_positions(self) -> Dict:
        """
        获取所有频道的BTC仓位信息
        
        Returns:
            Dict: 所有频道的BTC仓位信息
        """
        positions = {}
        for channel in self.btc_channel_positions:
            position_size = self.get_btc_position_size(channel)
            max_value = self.get_btc_max_position_value(channel)
            positions[channel] = {
                'position_size': position_size,
                'max_position_value': max_value,
                'ratio': self.btc_channel_positions[channel]
            }
        return positions

    def load_order_pairs(self) -> Dict:
        """
        加载订单配对关系
        
        Returns:
            Dict: 订单配对关系字典
        """
        try:
            if os.path.exists(self.order_pairs_file):
                with open(self.order_pairs_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"已加载 {len(data)} 条订单配对关系")
                    return data
            return {}
        except Exception as e:
            logger.error(f"加载订单配对关系失败: {e}")
            return {}

    def save_order_pairs(self):
        """
        保存订单配对关系到文件
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.order_pairs_file), exist_ok=True)
            
            # 保存记录
            with open(self.order_pairs_file, 'w', encoding='utf-8') as f:
                json.dump(self.order_pairs, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(self.order_pairs)} 条订单配对关系")
        except Exception as e:
            logger.error(f"保存订单配对关系失败: {e}")

    def is_symbol_supported(self, symbol: str) -> bool:
        """
        检查交易对是否支持
        
        Args:
            symbol: 交易对名称
            
        Returns:
            bool: 是否支持该交易对
        """
        try:
            # 标准化交易对名称
            if not symbol:
                return False
                
            # 移除可能的后缀（如USDT）
            base_symbol = str(symbol).strip().upper().replace('USDT', '')
            
            # 检查是否在支持的交易对中
            if base_symbol in SUPPORTED_SYMBOLS:
                return True
                
            # 检查是否是BTC
            if 'BTC' in base_symbol or base_symbol == 'BTC':
                return True
                
            # 检查是否是ETH
            if 'ETH' in base_symbol or base_symbol == 'ETH':
                return True
                
            logger.warning(f"不支持的交易对: {symbol}")
            return False
            
        except Exception as e:
            logger.error(f"检查交易对支持时出错: {e}")
            return False

def main():
    try:
        # 创建交易实例
        trader = BinanceTrader()
        
        # 获取全仓账户信息
        cross_margin_account = trader.get_cross_margin_account()
        
        # 获取当前持仓信息
        position_info = trader.get_position_info()
        
        # 显示交易配置
        print("\n当前交易配置:")
        print(json.dumps(trader.trading_config, indent=2))
        
        # 开始监控交易，每30秒检查一次
        trader.monitor_and_trade(interval=30)
        
    except Exception as e:
        logger.error(f"程序运行出错: {e}")

if __name__ == "__main__":
    main() 