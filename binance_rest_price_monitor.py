import json
import time
import urllib.request
import logging
from datetime import datetime
import pandas as pd

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BinanceRestPriceMonitor:
    def __init__(self, polling_interval=5):
        self.polling_interval = polling_interval  # 轮询间隔(秒)
        self.prices = {}  # 存储价格数据
        self.keep_running = False
    
    def start_monitoring(self, symbols):
        """开始监控指定的交易对价格"""
        if not isinstance(symbols, list):
            symbols = [symbols]
        
        self.keep_running = True
        
        logger.info(f"开始监控价格: {', '.join([s.upper() for s in symbols])}")
        
        try:
            while self.keep_running:
                for symbol in symbols:
                    price_data = self.get_price(symbol)
                    if price_data:
                        symbol_upper = symbol.upper()
                        self.prices[symbol_upper] = price_data
                        
                        # 显示价格信息
                        logger.info(f"{symbol_upper} 当前价格 - 买入: {price_data['bid']}, "
                                  f"卖出: {price_data['ask']}, 中间价: {price_data['mid']:.2f}")
                
                # 等待下一次轮询
                time.sleep(self.polling_interval)
        
        except KeyboardInterrupt:
            logger.info("手动中断监控")
        except Exception as e:
            logger.error(f"监控过程中发生错误: {e}")
        finally:
            self.keep_running = False
    
    def get_price(self, symbol):
        """获取特定交易对的价格数据"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={symbol.upper()}"
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode())
                
                if "bidPrice" in data and "askPrice" in data:
                    price_data = {
                        "bid": float(data["bidPrice"]),
                        "ask": float(data["askPrice"]),
                        "bid_qty": float(data["bidQty"]),
                        "ask_qty": float(data["askQty"]),
                        "timestamp": time.time()
                    }
                    price_data["mid"] = (price_data["bid"] + price_data["ask"]) / 2
                    return price_data
                    
        except Exception as e:
            logger.error(f"获取 {symbol} 价格时出错: {e}")
        
        return None
    
    def stop_monitoring(self):
        """停止监控"""
        self.keep_running = False
        logger.info("已停止价格监控")
    
    def get_current_price(self, symbol):
        """获取最近一次记录的价格"""
        symbol = symbol.upper()
        if symbol in self.prices:
            return self.prices[symbol]
        return None

    def start_monitoring_with_history(self, symbols, history_file=None):
        """开始监控价格并保存历史记录"""
        if not isinstance(symbols, list):
            symbols = [symbols]
        
        self.keep_running = True
        
        # 创建或加载历史记录文件
        if history_file:
            try:
                # 尝试加载现有文件
                self.history_df = pd.read_csv(history_file)
                logger.info(f"加载历史数据文件: {history_file}")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                # 创建新文件
                self.history_df = pd.DataFrame(columns=['timestamp', 'symbol', 'bid', 'ask', 'mid'])
                logger.info(f"创建新的历史数据文件: {history_file}")
        else:
            # 内存中存储
            self.history_df = pd.DataFrame(columns=['timestamp', 'symbol', 'bid', 'ask', 'mid'])
        
        logger.info(f"开始监控价格: {', '.join([s.upper() for s in symbols])}")
        
        try:
            while self.keep_running:
                for symbol in symbols:
                    price_data = self.get_price(symbol)
                    if price_data:
                        symbol_upper = symbol.upper()
                        self.prices[symbol_upper] = price_data
                        
                        # 显示价格信息
                        logger.info(f"{symbol_upper} 当前价格 - 买入: {price_data['bid']}, "
                                  f"卖出: {price_data['ask']}, 中间价: {price_data['mid']:.2f}")
                        
                        # 添加到历史记录
                        new_row = {
                            'timestamp': datetime.fromtimestamp(price_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                            'symbol': symbol_upper,
                            'bid': price_data['bid'],
                            'ask': price_data['ask'],
                            'mid': price_data['mid']
                        }
                        self.history_df = pd.concat([self.history_df, pd.DataFrame([new_row])], ignore_index=True)
                        
                        # 每100条记录保存一次文件
                        if history_file and len(self.history_df) % 100 == 0:
                            self.history_df.to_csv(history_file, index=False)
                            logger.debug(f"历史数据已保存到文件 ({len(self.history_df)}条记录)")
                
                # 等待下一次轮询
                time.sleep(self.polling_interval)
        
        except KeyboardInterrupt:
            logger.info("手动中断监控")
        except Exception as e:
            logger.error(f"监控过程中发生错误: {e}")
        finally:
            # 保存最终数据
            if history_file:
                self.history_df.to_csv(history_file, index=False)
                logger.info(f"历史数据已保存到 {history_file} ({len(self.history_df)}条记录)")
            
            self.keep_running = False
        
    def get_history(self, symbol=None, start_time=None, end_time=None):
        """获取历史数据
        
        参数:
        symbol (str): 交易对，如果为None，返回所有交易对数据
        start_time (str): 开始时间，格式 'YYYY-MM-DD HH:MM:SS'
        end_time (str): 结束时间，格式 'YYYY-MM-DD HH:MM:SS'
        """
        if not hasattr(self, 'history_df'):
            logger.error("没有历史数据")
            return pd.DataFrame()
        
        df = self.history_df.copy()
        
        # 筛选交易对
        if symbol:
            df = df[df['symbol'] == symbol.upper()]
        
        # 筛选时间范围
        if start_time:
            df = df[df['timestamp'] >= start_time]
        
        if end_time:
            df = df[df['timestamp'] <= end_time]
        
        return df


# 使用示例
if __name__ == "__main__":
    # 创建价格监控器
    monitor = BinanceRestPriceMonitor(polling_interval=3)  # 每3秒更新一次
    
    try:
        # 测试单个交易对价格获取
        btc_price = monitor.get_price("BTCUSDT")
        if btc_price:
            logger.info(f"BTC单次价格测试: {btc_price['mid']:.2f} USDT")
            
        # 开始持续监控多个交易对
        symbols_to_monitor = ["btcusdt", "ethusdt", "bnbusdt"]
        logger.info(f"开始监控以下交易对: {', '.join([s.upper() for s in symbols_to_monitor])}")
        
        # 启动持续监控
        monitor.start_monitoring(symbols_to_monitor)
        
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.exception(f"程序异常: {e}")
    finally:
        monitor.stop_monitoring() 