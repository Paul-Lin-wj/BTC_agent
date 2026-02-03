#https://pypi.org/project/websocket_client/
import websocket
import csv
import json
import os
import time
import threading
import sys
from datetime import datetime
from collections import defaultdict

# CSV文件路径
CSV_DIR = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data"
CSV_FILE = os.path.join(CSV_DIR, "stock_data.csv")

# 用于去重的集合：存储已写入的 (timestamp, symbol) 组合
written_records = set()

# 网络状态监控
last_data_time = None
is_connected = False
symbol_to_track = "BINANCE:BTCUSDT"
no_data_timeout = 10  # 超过10秒没数据认为是网络问题

# 重连配置
reconnect_delay = 1  # 重连延迟（秒）
max_reconnect_attempts = 0  # 0表示无限重试
reconnect_count = 0


# 初始化CSV文件（如果不存在则创建并写入表头）
def init_csv_file():
    global written_records

    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'datetime', 'symbol', 'price', 'volume'])
        print(f"CSV file created: {CSV_FILE}")
    else:
        print(f"CSV file already exists: {CSV_FILE}")
        # 加载已有记录到去重集合
        load_existing_records()


# 从现有CSV文件加载已有记录，用于去重
def load_existing_records():
    global written_records
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # 使用 (秒级时间戳, symbol) 作为唯一标识
                ms_timestamp = int(row['timestamp'])
                second_timestamp = ms_timestamp // 1000  # 转换为秒级时间戳
                key = (str(second_timestamp), row['symbol'])
                written_records.add(key)
                count += 1
        print(f"Loaded {count} existing records for deduplication (by second)")
    except Exception as e:
        print(f"Error loading existing records: {e}")


# 写入数据到CSV（带去重）
def write_to_csv(symbol, price, volume, timestamp, is_placeholder=False):
    global written_records

    # 将毫秒时间戳转换为秒级时间戳，用于去重（同一秒只保留第一条数据）
    second_timestamp = timestamp // 1000
    key = (str(second_timestamp), symbol)

    # 检查是否已存在相同秒级时间戳和symbol的记录
    if key in written_records:
        return  # 跳过重复记录

    # 将时间戳转换为可读时间
    dt = datetime.fromtimestamp(timestamp / 1000)  # 毫秒时间戳
    datetime_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留毫秒

    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, datetime_str, symbol, price, volume])

    # 记录到去重集合（使用秒级时间戳）
    written_records.add(key)

    if is_placeholder:
        print(f"[NO DATA] {symbol} | Price: 0 | Volume: 0 | Time: {datetime_str}")
    else:
        print(f"Saved: {symbol} | Price: {price} | Volume: {volume} | Time: {datetime_str}")


# 写入网络断开期间的占位数据
def write_placeholder_data():
    """在网络断开期间写入占位数据（价格为0，交易量为0）"""
    global last_data_time, symbol_to_track

    if last_data_time is None:
        return

    # 从最后数据时间开始，每秒写入一个占位记录
    current_time = int(time.time() * 1000)  # 当前时间戳（毫秒）
    placeholder_time = last_data_time + 1000  # 从最后数据时间+1秒开始

    while placeholder_time < current_time:
        write_to_csv(symbol_to_track, 0, 0, placeholder_time, is_placeholder=True)
        placeholder_time += 1000


def on_message(ws, message):
    global last_data_time, is_connected

    # 更新最后数据时间
    last_data_time = int(time.time() * 1000)
    is_connected = True

    # 解析JSON数据
    try:
        data = json.loads(message)

        # 检查是否是交易数据
        if data.get('type') == 'trade' and 'data' in data:
            trades = data['data']
            for trade in trades:
                symbol = trade.get('s', 'N/A')
                price = trade.get('p', 0)
                volume = trade.get('v', 0)
                timestamp = trade.get('t', 0)

                # 写入CSV（带去重检查）
                write_to_csv(symbol, price, volume, timestamp)
    except json.JSONDecodeError:
        # 如果不是JSON格式，直接打印原始消息
        print(message)


def on_error(ws, error):
    global is_connected
    print(f"WebSocket Error: {error}")
    is_connected = False


def on_close(ws, close_status_code, close_msg):
    global is_connected, reconnect_count
    print(f"### Connection closed (code: {close_status_code}, msg: {close_msg}) ###")
    is_connected = False

    # 写入断开期间的占位数据
    write_placeholder_data()

    # 完全重启程序
    print("[INFO] Connection lost, restarting program...")
    time.sleep(1)
    restart_program()


def on_open(ws):
    global is_connected, reconnect_count, last_data_time
    print("### Connected ###")
    is_connected = True
    reconnect_count = 0  # 重置重连计数
    last_data_time = int(time.time() * 1000)  # 更新最后数据时间

    # 订阅股票
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')


def restart_program():
    """完全重启当前程序"""
    print("[INFO] Restarting program...")
    # 使用 os.execv 重启当前进程，保持相同的参数
    os.execv(sys.executable, [sys.executable] + sys.argv)


# 网络状态监控线程
def monitor_network_status():
    """监控网络状态，当长时间没数据时重启程序"""
    global last_data_time, is_connected

    while True:
        time.sleep(5)  # 每5秒检查一次

        if last_data_time is None:
            continue

        elapsed = (time.time() * 1000) - last_data_time

        # 如果超过设定时间没有数据，认为网络有问题
        if elapsed > no_data_timeout * 1000 and is_connected:
            print(f"[WARNING] No data received for {elapsed/1000:.1f} seconds, possible network issue...")

        # 如果超过30秒没数据，写入占位数据并提示
        if elapsed > 30 * 1000:
            write_placeholder_data()
            last_data_time = int(time.time() * 1000)  # 更新时间避免重复写入
            print(f"[INFO] Wrote placeholder data for network gap ({elapsed/1000:.1f}s)")

        # 如果超过60秒没数据，重启程序
        if elapsed > 60 * 1000:
            print(f"[ERROR] No data for {elapsed/1000:.1f} seconds, restarting program...")
            restart_program()


# WebSocket全局变量，用于重连
ws_app = None


def reconnect_websocket():
    """重新建立WebSocket连接"""
    global ws_app, is_connected

    try:
        ws_app = websocket.WebSocketApp(
            "wss://ws.finnhub.io?token=d5ssulhr01qmiccbs4qgd5ssulhr01qmiccbs4r0",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws_app.on_open = on_open

        # 在新线程中运行WebSocket
        ws_thread = threading.Thread(target=ws_app.run_forever,
                                     kwargs={"ping_interval": 30, "ping_timeout": 10},
                                     daemon=True)
        ws_thread.start()
    except Exception as e:
        print(f"Reconnection failed: {e}")
        is_connected = False


if __name__ == "__main__":
    # 初始化CSV文件
    init_csv_file()

    # 启用WebSocket追踪（可选，设为False减少输出）
    websocket.enableTrace(False)

    # 启动网络监控线程
    monitor_thread = threading.Thread(target=monitor_network_status, daemon=True)
    monitor_thread.start()

    # 首次连接
    reconnect_websocket()

    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n### Program stopped by user ###")