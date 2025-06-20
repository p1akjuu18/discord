import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time
from datetime import datetime, timedelta
import logging
import concurrent.futures
import os
import urllib3

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_session_with_retries():
    """创建带有重试机制的会话"""
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=5,  # 最大重试次数
        backoff_factor=1,  # 重试间隔
        status_forcelist=[500, 502, 503, 504],  # 需要重试的HTTP状态码
    )
    
    # 创建适配器
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    # 将适配器应用到http和https
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def ensure_data_dir():
    """确保数据保存目录存在"""
    data_dir = "crypto_data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir

def get_historical_klines(symbol, interval, start_str, end_str=None):
    """获取历史K线数据"""
    
    endpoint = "https://api.binance.com/api/v3/klines"
    
    # 转换日期字符串为时间戳
    start_ts = int(datetime.strptime(start_str, '%Y-%m-%d').timestamp() * 1000)
    if end_str:
        end_ts = int(datetime.strptime(end_str, '%Y-%m-%d').timestamp() * 1000)
    else:
        end_ts = int(time.time() * 1000)
    
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": 1000
    }
    
    try:
        # 使用带重试机制的会话
        session = create_session_with_retries()
        
        # 发送请求时禁用SSL验证
        response = session.get(endpoint, params=params, verify=False)
        response.raise_for_status()
        
        # 处理响应数据
        data = response.json()
        
        # 转换为DataFrame
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                       'close_time', 'quote_asset_volume', 'number_of_trades',
                                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                       'ignore'])
        
        # 转换时间戳
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 转换数值类型
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"获取数据时出错: {str(e)}")
        return pd.DataFrame()  # 返回空DataFrame而不是None
    finally:
        session.close()

def fetch_coin_history(symbol, start_date, end_date, interval='1m'):
    """获取单个币种的历史数据并保存"""
    logger.info(f"开始获取 {symbol} 的历史数据...")
    
    # 将时间范围分成每30天一段
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    data_dir = ensure_data_dir()
    filename = os.path.join(data_dir, f"{symbol.lower()}_history.csv")
    total_records = 0
    
    # 如果文件已存在，先删除
    if os.path.exists(filename):
        os.remove(filename)
    
    # 分段获取数据
    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=30), end_dt)
        
        logger.info(f"{symbol} 正在获取 {current_start.strftime('%Y-%m-%d')} 至 {current_end.strftime('%Y-%m-%d')} 的数据...")
        
        df = get_historical_klines(
            symbol,
            interval,
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        )
        
        if not df.empty:
            # 如果是第一次写入，包含表头；后续追加不包含表头
            df.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)
            total_records += len(df)
            logger.info(f"{symbol} 已保存 {total_records} 条记录到 {filename}")
        
        current_start = current_end + timedelta(seconds=1)
        time.sleep(1)  # 避免触发频率限制
    
    logger.info(f"{symbol} 全部数据获取完成！总共 {total_records} 条记录")
    return total_records

def fetch_single_coin(symbol, start_date, end_date, interval='1h'):
    """获取单个币种的数据（简单版本）"""
    logger.info(f"开始获取 {symbol} 的数据...")
    df = get_historical_klines(symbol, interval, start_date, end_date)
    
    if not df.empty:
        data_dir = ensure_data_dir()
        filename = os.path.join(data_dir, f"{symbol.lower()}_history.csv")
        df.to_csv(filename, index=False)
        logger.info(f"数据已保存到 {filename}")
        return len(df)
    else:
        logger.warning("未获取到数据")
        return 0

def detect_gaps(df, interval='1m'):
    """
    检测数据中的时间缺口
    
    参数:
    df (DataFrame): 包含时间序列数据的DataFrame
    interval (str): 数据间隔，如'1m', '1h'等
    
    返回:
    list: 包含(开始时间, 结束时间, 缺失分钟数)元组的列表
    """
    if df.empty:
        return []
    
    # 确保数据按时间排序
    df = df.sort_values('timestamp')
    
    # 根据间隔计算期望的时间差（以分钟为单位）
    interval_minutes = 1  # 默认为1分钟
    if interval.endswith('m'):
        interval_minutes = int(interval[:-1])
    elif interval.endswith('h'):
        interval_minutes = int(interval[:-1]) * 60
    elif interval.endswith('d'):
        interval_minutes = int(interval[:-1]) * 60 * 24
    
    # 计算连续行之间的时间差
    df['next_timestamp'] = df['timestamp'].shift(-1)
    df['time_diff'] = (df['next_timestamp'] - df['timestamp']).dt.total_seconds() / 60
    
    # 找出超过预期间隔的差距
    gaps = df[df['time_diff'] > interval_minutes * 1.5]  # 允许一定的误差
    
    # 记录所有时间缺口
    gap_list = []
    for _, row in gaps.iterrows():
        start_time = row['timestamp']
        end_time = row['next_timestamp']
        missing_minutes = round(row['time_diff']) - interval_minutes
        gap_list.append((start_time, end_time, missing_minutes))
    
    return gap_list

def fill_gaps(symbol, gaps, interval='1m'):
    """
    填补数据缺口
    
    参数:
    symbol (str): 币种符号
    gaps (list): 包含(开始时间, 结束时间, 缺失分钟数)元组的列表
    interval (str): 数据间隔，如'1m', '1h'等
    
    返回:
    int: 成功填补的缺口数量
    """
    filled_count = 0
    
    for start_time, end_time, _ in gaps:
        # 将datetime转换为字符串格式
        start_str = start_time.strftime('%Y-%m-%d')
        end_str = end_time.strftime('%Y-%m-%d')
        
        # 如果开始和结束是同一天，尝试获取这一天的完整数据
        if start_str == end_str:
            logger.info(f"正在填补 {symbol} 在 {start_str} 的数据缺口...")
        else:
            logger.info(f"正在填补 {symbol} 从 {start_str} 到 {end_str} 的数据缺口...")
        
        # 获取缺失时段的数据
        df = get_historical_klines(symbol, interval, start_str, end_str)
        
        if not df.empty:
            # 将数据追加到文件
            data_dir = ensure_data_dir()
            filename = os.path.join(data_dir, f"{symbol.lower()}_history.csv")
            
            # 读取现有文件
            if os.path.exists(filename):
                existing_df = pd.read_csv(filename)
                
                # 转换时间戳列为datetime
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                
                # 合并数据并去重
                combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['timestamp'])
                
                # 按时间排序
                combined_df = combined_df.sort_values('timestamp')
                
                # 保存合并后的数据
                combined_df.to_csv(filename, index=False)
                logger.info(f"成功填补 {symbol} 从 {start_str} 到 {end_str} 的数据缺口，新增 {len(df)} 条记录")
                filled_count += 1
            else:
                df.to_csv(filename, index=False)
                logger.info(f"无现有数据文件，已创建新文件并添加 {len(df)} 条记录")
                filled_count += 1
                
            # 避免请求过于频繁
            time.sleep(1)
    
    return filled_count

def check_and_repair_data(symbols=None, interval='1m'):
    """
    检查并修复指定币种的数据缺口
    
    参数:
    symbols (list): 要检查的币种列表，如果为None，则检查crypto_data目录下的所有文件
    interval (str): 数据间隔，如'1m', '1h'等
    
    返回:
    dict: 包含每个币种检查和修复结果的字典
    """
    data_dir = ensure_data_dir()
    results = {}
    
    # 如果没有指定币种，则检查目录下的所有文件
    if symbols is None:
        # 获取目录中所有以_history.csv结尾的文件
        files = [f for f in os.listdir(data_dir) if f.endswith('_history.csv')]
        symbols = [f.replace('_history.csv', '').upper() for f in files]
    
    logger.info(f"开始检查 {len(symbols)} 个币种的数据完整性...")
    
    for symbol in symbols:
        filename = os.path.join(data_dir, f"{symbol.lower()}_history.csv")
        
        if not os.path.exists(filename):
            logger.warning(f"{symbol} 的数据文件不存在，跳过")
            continue
        
        logger.info(f"正在检查 {symbol} 的数据...")
        
        try:
            # 读取数据
            df = pd.read_csv(filename)
            
            # 转换时间戳列为datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 检测时间缺口
            gaps = detect_gaps(df, interval)
            
            if gaps:
                total_missing_minutes = sum(minutes for _, _, minutes in gaps)
                total_duration = (df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 60
                missing_percentage = (total_missing_minutes / total_duration) * 100
                
                logger.info(f"{symbol} 数据范围: {df['timestamp'].min()} 至 {df['timestamp'].max()}")
                logger.info(f"{symbol} 检测到 {len(gaps)} 处时间缺口，约 {total_missing_minutes} 分钟数据缺失 ({missing_percentage:.2f}%)")
                
                # 尝试修复缺口
                filled_count = fill_gaps(symbol, gaps, interval)
                
                results[symbol] = {
                    "filename": f"{symbol.lower()}_history.csv",
                    "start_date": df['timestamp'].min(),
                    "end_date": df['timestamp'].max(),
                    "gaps": gaps,
                    "total_missing_minutes": total_missing_minutes,
                    "missing_percentage": missing_percentage,
                    "filled_count": filled_count
                }
            else:
                logger.info(f"{symbol} 数据完整，无缺失")
                results[symbol] = {
                    "filename": f"{symbol.lower()}_history.csv",
                    "start_date": df['timestamp'].min(),
                    "end_date": df['timestamp'].max(),
                    "gaps": [],
                    "total_missing_minutes": 0,
                    "missing_percentage": 0,
                    "filled_count": 0
                }
            
        except Exception as e:
            logger.error(f"检查 {symbol} 数据时出错: {str(e)}")
            results[symbol] = {"error": str(e)}
    
    return results

def generate_gaps_report(results):
    """
    生成数据缺口报告并保存到CSV文件
    
    参数:
    results (dict): check_and_repair_data函数的返回结果
    
    返回:
    str: 报告文件路径
    """
    data_dir = ensure_data_dir()
    output_folder = os.path.join(data_dir, 'reports')
    
    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # 收集所有缺口数据
    all_gaps = []
    
    for symbol, result in results.items():
        if "error" in result:
            continue
            
        for start_time, end_time, minutes in result["gaps"]:
            all_gaps.append({
                "币种": symbol,
                "开始时间": start_time,
                "结束时间": end_time,
                "缺失分钟数": minutes,
                "是否已修复": minutes <= result["filled_count"]
            })
    
    # 如果没有缺口，返回None
    if not all_gaps:
        logger.info("所有数据完整，无缺失")
        return None
    
    # 创建DataFrame
    gaps_df = pd.DataFrame(all_gaps)
    
    # 生成报告文件名（包含时间戳）
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"data_gaps_report_{timestamp}.csv"
    report_path = os.path.join(output_folder, report_filename)
    
    # 保存报告
    gaps_df.to_csv(report_path, index=False, encoding='utf-8-sig')
    logger.info(f"数据缺口报告已保存至: {report_path}")
    
    return report_path

def main():
    # 设置参数
    # 直接从桌面读取symbol.csv文件的symbol列
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    symbol_file_path = os.path.join(desktop_path, "symbols.csv")
    
    # 默认币种列表，当文件不存在时使用
    default_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT', 'DOGEUSDT', 'XRPUSDT', 'DOTUSDT']
    
    if os.path.exists(symbol_file_path):
        try:
            # 尝试读取CSV文件的symbol列
            df = pd.read_csv(symbol_file_path)
            if 'symbol' in df.columns:
                symbols = df['symbol'].tolist()
                logger.info(f"从CSV文件成功读取了{len(symbols)}个币种符号")
            else:
                logger.warning("CSV文件中没有找到'symbol'列，使用默认币种列表")
                symbols = default_symbols
        except Exception as e:
            logger.error(f"读取symbols.csv文件出错: {str(e)}")
            symbols = default_symbols
    else:
        logger.warning(f"未找到文件: {symbol_file_path}，使用默认币种列表")
        symbols = default_symbols
    
    start_date = '2023-01-01'  # 扩大时间范围，从2023年开始
    end_date = datetime.now().strftime('%Y-%m-%d')  # 使用当前日期作为结束日期
    interval = '1m'  # 使用1分钟的间隔获取更详细的数据
    
    logger.info("开始获取历史数据...")
    logger.info(f"时间范围: {start_date} 至 {end_date}")
    logger.info(f"时间间隔: {interval}")
    logger.info(f"数据将保存在: {os.path.abspath(ensure_data_dir())} 目录下")
    
    # 使用线程池并行获取数据，增加线程数以加快处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_coin_history, symbol, start_date, end_date, interval): symbol for symbol in symbols}
        
        total_records = {}
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                records = future.result()
                total_records[symbol] = records
            except Exception as e:
                logger.error(f"{symbol} 数据获取出错: {str(e)}")
    
    logger.info("\n所有数据获取完成！")
    logger.info("统计信息:")
    for symbol, records in total_records.items():
        logger.info(f"{symbol}: {records} 条记录")
    
    # 检查数据完整性
    logger.info("\n开始检查数据完整性...")
    check_results = check_and_repair_data(symbols, interval)
    
    # 生成报告
    report_path = generate_gaps_report(check_results)
    
    if report_path:
        logger.info(f"数据缺口报告已生成: {report_path}")
    else:
        logger.info("所有数据已完整，无需生成报告")

if __name__ == "__main__":
    main()