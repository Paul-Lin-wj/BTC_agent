# -*- coding: utf-8 -*-
"""
股票监控智能体系统 - 集成版
整合了数据获取管理、数据分析、图表生成、BTC实时价格查询等功能
"""

import os
import sys
import json
import asyncio
import csv
import subprocess
import signal
import websocket
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# 关闭 loguru 的 DEBUG 日志
from loguru import logger as loguru_logger
loguru_logger.remove()
loguru_logger.add(lambda _: None, level="WARNING")
loguru_logger.add(sys.stderr, level="WARNING")

# 添加项目路径
sys.path.insert(0, '/data/juno/lin/agent/drsai-main')
sys.path.insert(0, '/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python')

from drsai import AssistantAgent, HepAIChatCompletionClient
from drsai.backend import run_worker, run_console
from drsai.modules.managers.database import DatabaseManager
from drsai import tools_recycle_reply_function

import finnhub

# ==================== 配置 ====================
DEBUG_MODE = False  # True=命令行模式, False=后端API服务模式

# CSV文件路径
CSV_FILE = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

# 图表保存目录
CHART_DIR = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/charts"
os.makedirs(CHART_DIR, exist_ok=True)

# data_get.py 脚本路径
DATA_GET_SCRIPT = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python/data_get.py"
PYTHON_EXECUTABLE = "/datafs/users/lin/python-venv/drsai/bin/python"

# ==================== Matplotlib配置 ====================
# 设置matplotlib使用非GUI后端
matplotlib.use('Agg')

# 配置中文字体
import matplotlib.font_manager as fm
available_fonts = set([f.name for f in fm.fontManager.ttflist])

preferred_fonts = [
    'Noto Sans CJK SC', 'Noto Sans CJK JP', 'Noto Serif CJK JP',
    'WenQuanYi Zen Hei Sharp', 'AR PL UMing CN', 'SimSun', 'SimHei',
]

selected_font = None
for font in preferred_fonts:
    if font in available_fonts:
        selected_font = font
        break

if selected_font:
    plt.rcParams['font.sans-serif'] = [selected_font, 'DejaVu Sans']
else:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans']

plt.rcParams['axes.unicode_minus'] = False

# ==================== 数据处理辅助函数 ====================

def _apply_time_filter_df(df: pd.DataFrame, time_filter: str = None) -> pd.DataFrame:
    """应用时间过滤到DataFrame"""
    if not time_filter:
        return df

    import re
    now = datetime.now()
    time_filter_original = time_filter.strip()
    time_filter = time_filter_original.lower()

    # 按分钟过滤
    if 'min' in time_filter or '分钟' in time_filter:
        match = re.search(r'(\d+)\s*(min|分钟)', time_filter)
        if match:
            mins = int(match.group(1))
            start = now - timedelta(minutes=mins)
            return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        cn_min_map = {'六十': 60, '五十': 50, '四十': 40, '三十': 30,
                      '二十': 20, '十五': 15, '十二': 12, '十一': 11,
                      '十': 10, '九': 9, '八': 8, '七': 7, '六': 6,
                      '五': 5, '四': 4, '三': 3, '二': 2, '两': 2, '一': 1}
        for cn_num, mins in cn_min_map.items():
            if cn_num in time_filter and '分钟' in time_filter:
                start = now - timedelta(minutes=mins)
                return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()

    # 按小时过滤
    hour_patterns = [
        ('过去一小时', '1h', '1小时', 1),
        ('2h', '2小时', '两小时', 2),
        ('3h', '3小时', '三小时', 3),
        ('6h', '6小时', '六小时', 6),
        ('12h', '12小时', '十二小时', 12),
        ('24h', '24小时', '二十四小时', 24),
    ]
    for pattern in hour_patterns:
        if any(p in time_filter for p in pattern[:-1]):
            start = now - timedelta(hours=pattern[-1])
            return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()

    # 按天过滤
    if any(kw in time_filter for kw in ['今天', '今日']):
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        filtered = df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        if filtered.empty:
            start = now - timedelta(hours=24)
            filtered = df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        return filtered
    elif '昨天' in time_filter and '点' not in time_filter:
        yesterday = now - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()

    # 具体时间段解析
    cn_hour_map = {
        '零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4,
        '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
        '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '二十一': 21, '二十二': 22, '二十三': 23
    }

    day_offset = 0
    if '昨天' in time_filter:
        day_offset = -1
    elif '前天' in time_filter:
        day_offset = -2
    elif '明天' in time_filter:
        day_offset = 1

    is_afternoon = '下午' in time_filter
    is_morning = '上午' in time_filter or '凌晨' in time_filter or '早上' in time_filter

    # 匹配阿拉伯数字小时范围
    range_match = re.search(r'(\d{1,2})\s*点.*?(\d{1,2})\s*点', time_filter)
    if range_match:
        h1 = int(range_match.group(1))
        h2 = int(range_match.group(2))
        if is_afternoon and h1 < 12:
            h1 += 12
        if is_afternoon and h2 < 12:
            h2 += 12
        base_date = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        start = base_date + timedelta(hours=h1)
        end = base_date + timedelta(hours=h2)
        return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()

    # 匹配单点小时
    hour_match = re.search(r'(\d{1,2})\s*点[^到至]', time_filter)
    if not hour_match:
        hour_match = re.search(r'(\d{1,2})\s*点$', time_filter)
    if hour_match:
        hour = int(hour_match.group(1))
        if is_afternoon and hour < 12:
            hour += 12
        elif is_morning and hour == 12:
            hour = 0
        base_date = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        start = base_date + timedelta(hours=hour)
        end = base_date + timedelta(hours=hour+1)
        return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()

    return df


# ==================== 工具函数：数据获取管理 ====================

def check_data_quality(check_count: int = 10) -> str:
    """检查数据质量"""
    try:
        if not os.path.exists(CSV_FILE):
            return f"## 数据质量检查\n❌ 数据文件不存在"

        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return "## 数据质量检查\n❌ 数据文件为空"

        latest_rows = rows[-check_count:] if len(rows) >= check_count else rows
        zero_count = sum(1 for r in latest_rows if float(r.get('price', 0)) == 0)
        valid_count = len(latest_rows) - zero_count

        latest_time = latest_rows[-1].get('datetime', 'Unknown')
        latest_price = float(latest_rows[-1].get('price', 0))

        if zero_count == len(latest_rows):
            return f"""## 数据质量检查

**状态:** ⚠️ 异常
**问题:** 最新数据全是占位数据（price=0）
**检查数量:** {len(latest_rows)}条
**有效数据:** 0条

**建议:** 请重启数据获取服务

检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        elif zero_count > len(latest_rows) // 2:
            return f"""## 数据质量检查

**状态:** ⚠️ 部分异常
**问题:** 最新{len(latest_rows)}条中有{zero_count}条占位数据
**有效数据:** {valid_count}/{len(latest_rows)}条
**最新价格:** {latest_price:.2f} USDT

**建议:** 请检查网络连接

检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        else:
            return f"""## 数据质量检查

**状态:** ✅ 正常
**检查数量:** {len(latest_rows)}条
**有效数据:** {valid_count}条
**最新价格:** {latest_price:.2f} USDT
**最新时间:** {latest_time}

检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 数据质量检查\n检查失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def get_data_collection_status(check_count: int = 10) -> str:
    """获取数据获取服务状态"""
    try:
        # 先检查进程状态
        result = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True, timeout=5)

        if result.returncode == 0:
            running_pids = result.stdout.strip().split('\n')

            # 简化数据质量检查 - 只检查文件是否存在和最后修改时间
            quality_msg = ""
            try:
                if os.path.exists(CSV_FILE):
                    mtime = os.path.getmtime(CSV_FILE)
                    mod_time = datetime.fromtimestamp(mtime)
                    time_diff = (datetime.now() - mod_time).total_seconds()

                    # 读取最后一行获取价格
                    with open(CSV_FILE, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    if len(lines) > 1:
                        last_line = lines[-1].strip()
                        parts = last_line.split(',')
                        if len(parts) >= 2:
                            try:
                                latest_price = float(parts[1])
                                latest_dt = parts[0]
                                quality_msg = f"\n**最新价格:** {latest_price:.2f} USDT\n**更新时间:** {latest_dt}\n**数据更新:** {int(time_diff)}秒前"
                            except:
                                quality_msg = f"\n**数据更新:** {int(time_diff)}秒前"
                        else:
                            quality_msg = f"\n**数据更新:** {int(time_diff)}秒前"
                    else:
                        quality_msg = "\n**数据质量:** 文件为空"
                else:
                    quality_msg = "\n**数据质量:** 文件不存在"
            except Exception as e:
                quality_msg = f"\n**数据质量:** 检查失败"

            return f"""## 数据获取服务状态

**状态:** 🟢 运行中
**进程ID:** {', '.join(running_pids)}{quality_msg}

查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        else:
            return f"""## 数据获取服务状态

**状态:** 🔴 未运行

提示：使用"启动数据获取服务"命令启动

查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except subprocess.TimeoutExpired:
        return f"## 数据获取服务状态\n❌ 查询超时\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    except Exception as e:
        return f"## 数据获取服务状态\n❌ 查询失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def start_data_collection(duration: int = 0) -> str:
    """启动数据获取服务"""
    try:
        if not os.path.exists(DATA_GET_SCRIPT):
            return f"## 启动数据获取服务\n失败：脚本不存在 {DATA_GET_SCRIPT}"

        # 检查是否已运行
        existing = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True)
        if existing.returncode == 0:
            pids = existing.stdout.strip().split('\n')
            return f"""## 启动数据获取服务

**状态:** 已在运行中
**进程ID:** {', '.join(pids)}

无需重复启动

时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""

        output_file = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python/nohup.out"
        cmd = f"nohup {PYTHON_EXECUTABLE} {DATA_GET_SCRIPT} > {output_file} 2>&1 &"
        subprocess.run(cmd, shell=True, check=True)

        import time
        time.sleep(2)

        pid_result = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True)
        if pid_result.returncode == 0:
            pids = pid_result.stdout.strip().split('\n')
            latest_pid = pids[-1] if pids else "Unknown"
        else:
            latest_pid = "Unknown"

        return f"""## 启动数据获取服务

**状态:** ✅ 启动成功
**进程ID:** {latest_pid}
**启动时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 启动数据获取服务\n失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def stop_data_collection() -> str:
    """停止数据获取服务"""
    try:
        result = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True)
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                os.kill(int(pid), signal.SIGTERM)
            return f"""## 停止数据获取服务

**状态:** ✅ 已停止
**已停止进程:** {', '.join(pids)}

时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        else:
            return f"## 停止数据获取服务\n状态: 服务未在运行\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    except Exception as e:
        return f"## 停止数据获取服务\n失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def restart_data_collection() -> str:
    """重启数据获取服务"""
    # 先停止
    stop_info = ""
    try:
        result = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True)
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                os.kill(int(pid), signal.SIGTERM)
            stop_info = f"已停止进程: {', '.join(pids)}"
    except:
        stop_info = "停止失败或无进程"

    import time
    time.sleep(1)

    # 再启动
    try:
        if not os.path.exists(DATA_GET_SCRIPT):
            return f"## 重启数据获取服务\n失败：脚本不存在"

        cmd = f"nohup {PYTHON_EXECUTABLE} {DATA_GET_SCRIPT} > /dev/null 2>&1 &"
        subprocess.run(cmd, shell=True, check=True)
        time.sleep(2)

        pid_result = subprocess.run(["pgrep", "-f", "data_get.py"], capture_output=True, text=True)
        if pid_result.returncode == 0:
            pids = pid_result.stdout.strip().split('\n')
            new_pid = pids[-1] if pids else "Unknown"
        else:
            new_pid = "Unknown"

        return f"""## 重启数据获取服务

**状态:** ✅ 重启成功
**新进程ID:** {new_pid}
**{stop_info}**

时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 重启数据获取服务\n失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def get_csv_file_info() -> str:
    """获取CSV文件信息"""
    try:
        if not os.path.exists(CSV_FILE):
            return f"## 数据文件信息\n文件不存在: {CSV_FILE}"

        with open(CSV_FILE, 'r') as f:
            line_count = sum(1 for _ in f)
        file_size = os.path.getsize(CSV_FILE)
        mtime = os.path.getmtime(CSV_FILE)
        mod_time = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

        # 格式化文件大小
        if file_size < 1024:
            size_str = f"{file_size} B"
        elif file_size < 1024 * 1024:
            size_str = f"{file_size / 1024:.2f} KB"
        else:
            size_str = f"{file_size / (1024 * 1024):.2f} MB"

        return f"""## 数据文件信息

**文件路径:** {CSV_FILE}
**记录数:** {line_count - 1}条
**文件大小:** {size_str}
**最后修改:** {mod_time}

查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 数据文件信息\n读取失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


# ==================== 工具函数：数据分析 ====================

def get_basic_stats(limit: int = 1000, time_filter: str = None) -> str:
    """获取基本统计数据"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## 基本统计\n❌ 数据文件不存在"

        df_raw = pd.read_csv(CSV_FILE)
        if df_raw.empty:
            return "## 基本统计\n❌ 数据文件为空"

        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        if time_filter:
            df_raw = _apply_time_filter_df(df_raw, time_filter)

        if df_raw.empty:
            return f"## 基本统计\n❌ 时间范围 '{time_filter}' 内没有数据"

        df_valid = df_raw[df_raw['price'] > 0].copy()
        if df_valid.empty:
            return "## 基本统计\n❌ 没有有效交易数据"

        price_min = df_valid['price'].min()
        price_max = df_valid['price'].max()
        price_mean = df_valid['price'].mean()
        price_median = df_valid['price'].median()
        price_std = df_valid['price'].std()

        volume_mean = df_valid['volume'].mean()
        volume_total = df_valid['volume'].sum()

        first_price = df_valid.iloc[0]['price']
        last_price = df_valid.iloc[-1]['price']
        price_change = last_price - first_price
        price_change_pct = (price_change / first_price) * 100

        change_symbol = "+" if price_change >= 0 else ""
        return f"""## 基本统计结果

### 数据概况
- 分析记录数: {len(df_valid)} 条
- 时间范围: {df_raw['datetime'].min().strftime('%H:%M:%S')} - {df_raw['datetime'].max().strftime('%H:%M:%S')}

### 价格统计
- 最低价: {round(price_min, 2)}
- 最高价: {round(price_max, 2)}
- 平均价: {round(price_mean, 2)}
- 中位数: {round(price_median, 2)}
- 价格波动: {round(price_std, 2)}
- 价格变化: {first_price:.2f} → {last_price:.2f} ({change_symbol}{round(price_change, 2)} / {change_symbol}{round(price_change_pct, 3)}%)

### 成交量统计
- 平均成交量: {round(volume_mean, 6)}
- 总成交量: {round(volume_total, 6)}

分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 基本统计\n❌ 分析失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def analyze_trend(limit: int = 1000, time_filter: str = None) -> str:
    """分析价格趋势"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## 趋势分析\n❌ 数据文件不存在"

        df_raw = pd.read_csv(CSV_FILE)
        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        if time_filter:
            df_raw = _apply_time_filter_df(df_raw, time_filter)

        if df_raw.empty:
            return f"## 趋势分析\n❌ 时间范围 '{time_filter}' 内没有数据"

        df_valid = df_raw[df_raw['price'] > 0].copy()
        if len(df_valid) < 2:
            return "## 趋势分析\n❌ 数据不足，无法分析趋势"

        prices = df_valid['price'].values
        first_price = prices[0]
        last_price = prices[-1]
        total_change = last_price - first_price
        total_change_pct = (total_change / first_price) * 100

        trend = "震荡"
        trend_strength = "弱"
        if total_change_pct > 0.5:
            trend = "上涨"
            if total_change_pct > 2:
                trend_strength = "强"
            elif total_change_pct > 1:
                trend_strength = "中"
        elif total_change_pct < -0.5:
            trend = "下跌"
            if total_change_pct < -2:
                trend_strength = "强"
            elif total_change_pct < -1:
                trend_strength = "中"

        up_moves = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i-1])
        down_moves = sum(1 for i in range(1, len(prices)) if prices[i] < prices[i-1])
        up_ratio = (up_moves / (up_moves + down_moves) * 100) if (up_moves + down_moves) > 0 else 50

        change_symbol = "+" if total_change >= 0 else ""
        return f"""## 趋势分析结果

### 总体趋势
- 趋势方向: **{trend}** ({trend_strength})
- 价格变化: {round(first_price, 2)} → {round(last_price, 2)} ({change_symbol}{round(total_change, 2)} / {change_symbol}{round(total_change_pct, 3)}%)

### 波动分析
- 上涨次数: {up_moves}
- 下跌次数: {down_moves}
- 上涨占比: {round(up_ratio, 1)}%

### 总结
共分析 {len(df_valid)} 条记录，价格呈{'上升' if trend == '上涨' else '下降' if trend == '下跌' else '中性'}趋势。

分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 趋势分析失败\n❌ 错误: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def analyze_volatility(limit: int = 1000, time_filter: str = None) -> str:
    """分析价格波动性"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## 波动性分析\n❌ 数据文件不存在"

        df_raw = pd.read_csv(CSV_FILE)
        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        if time_filter:
            df_raw = _apply_time_filter_df(df_raw, time_filter)

        if df_raw.empty:
            return f"## 波动性分析\n❌ 时间范围 '{time_filter}' 内没有数据"

        df_valid = df_raw[df_raw['price'] > 0].copy()
        if len(df_valid) < 2:
            return "## 波动性分析\n❌ 数据不足"

        prices = df_valid['price'].values
        mean_price = np.mean(prices)
        std_dev = np.std(prices)
        cv = (std_dev / mean_price) * 100 if mean_price > 0 else 0

        volatility_level = "低"
        if cv > 1:
            volatility_level = "极高"
        elif cv > 0.5:
            volatility_level = "高"
        elif cv > 0.2:
            volatility_level = "中"

        true_range = df_valid['price'].max() - df_valid['price'].min()
        true_range_pct = (true_range / df_valid['price'].min()) * 100

        return f"""## 波动性分析结果

### 波动等级
- 波动水平: **{volatility_level}**

### 统计指标
- 标准差: {round(std_dev, 2)}
- 变异系数: {round(cv, 4)}%
- 平均价格: {round(mean_price, 2)}

### 真实波动幅度
- 绝对幅度: {round(true_range, 2)}
- 相对幅度: {round(true_range_pct, 4)}%

### 风险评估
当前波动性等级为 **{volatility_level}** {'(高风险)' if volatility_level in ['高', '极高'] else '(相对稳定)'}。

分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 波动性分析失败\n❌ 错误: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def analyze_time_distribution(limit: int = 10000, time_filter: str = None) -> str:
    """分析时间分布"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## 时间分布分析\n❌ 数据文件不存在"

        df = pd.read_csv(CSV_FILE)
        df['datetime'] = pd.to_datetime(df['datetime'])

        if time_filter:
            df = _apply_time_filter_df(df, time_filter)

        df_valid = df[df['price'] > 0].copy()
        if df_valid.empty:
            return "## 时间分布分析\n❌ 没有有效数据"

        df_valid['hour'] = df_valid['datetime'].dt.hour

        hourly_stats = df_valid.groupby('hour').agg({
            'price': ['mean', 'min', 'max', 'count'],
            'volume': 'sum'
        }).round(2)
        hourly_stats.columns = ['avg_price', 'min_price', 'max_price', 'trade_count', 'total_volume']

        most_active = hourly_stats['trade_count'].idxmax()
        least_active = hourly_stats['trade_count'].idxmin()

        output = f"""## 时间分布分析结果

### 概述
- 覆盖小时数: {len(hourly_stats)}
- 最活跃时段: {most_active}:00
- 最不活跃时段: {least_active}:00

### 按小时统计 (前5)
| 时段 | 平均价 | 最低价 | 最高价 | 交易次数 | 总成交量 |
|------|--------|--------|--------|----------|----------|"""
        top_hours = sorted(hourly_stats.to_dict('records'), key=lambda x: x['trade_count'], reverse=True)[:5]
        for h in top_hours:
            output += f"\n| {int(h['hour'])}:00 | {h['avg_price']:.2f} | {h['min_price']:.2f} | {h['max_price']:.2f} | {int(h['trade_count'])} | {h['total_volume']:.4f} |"

        output += f"\n\n分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        return output
    except Exception as e:
        return f"## 时间分布分析失败\n❌ 错误: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def compare_time_periods(period1: str = "过去一小时", period2: str = "过去一小时前一小时") -> str:
    """比较两个时间段"""
    try:
        now = datetime.now()

        def parse_period(p: str):
            p = p.lower().strip()
            if '过去一小时' in p or '1h' in p or '1小时' in p:
                return now - timedelta(hours=1), now
            elif '过去两小时' in p or '2h' in p:
                return now - timedelta(hours=2), now - timedelta(hours=1)
            elif '今天' in p or '今日' in p:
                start = now.replace(hour=0, minute=0, second=0)
                return start, now
            elif '昨天' in p:
                yesterday = now - timedelta(days=1)
                start = yesterday.replace(hour=0, minute=0, second=0)
                end = yesterday.replace(hour=23, minute=59, second=59)
                return start, end
            return None, None

        start1, end1 = parse_period(period1)
        start2, end2 = parse_period(period2)

        if not start1 or not start2:
            return "## 时间段比较\n❌ 无法解析时间段"

        df = pd.read_csv(CSV_FILE)
        df['datetime'] = pd.to_datetime(df['datetime'])

        df1 = df[(df['datetime'] >= start1) & (df['datetime'] <= end1)].copy()
        df2 = df[(df['datetime'] >= start2) & (df['datetime'] <= end2)].copy()
        df1 = df1[df1['price'] > 0]
        df2 = df2[df2['price'] > 0]

        if df1.empty or df2.empty:
            return "## 时间段比较\n❌ 某个时间段没有数据"

        stats1 = {
            "avg_price": df1['price'].mean(),
            "volume": df1['volume'].sum(),
            "records": len(df1)
        }
        stats2 = {
            "avg_price": df2['price'].mean(),
            "volume": df2['volume'].sum(),
            "records": len(df2)
        }

        avg_change = stats1['avg_price'] - stats2['avg_price']
        avg_change_pct = (avg_change / stats2['avg_price'] * 100) if stats2['avg_price'] > 0 else 0
        vol_change = stats1['volume'] - stats2['volume']
        vol_change_pct = (vol_change / stats2['volume'] * 100) if stats2['volume'] > 0 else 0

        price_sym = "+" if avg_change >= 0 else ""
        vol_sym = "+" if vol_change >= 0 else ""
        return f"""## 时间段比较结果

### {period1}
- 平均价: {round(stats1['avg_price'], 2)}
- 总成交量: {round(stats1['volume'], 6)}
- 记录数: {stats1['records']}

### {period2}
- 平均价: {round(stats2['avg_price'], 2)}
- 总成交量: {round(stats2['volume'], 6)}
- 记录数: {stats2['records']}

### 差异分析
- 平均价变化: {price_sym}{round(avg_change, 2)} ({price_sym}{round(avg_change_pct, 3)}%)
- 成交量变化: {vol_sym}{round(vol_change, 6)} ({vol_sym}{round(vol_change_pct, 3)}%)

分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 时间段比较失败\n❌ 错误: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def detect_price_anomalies(limit: int = 1000, threshold: float = 2.0, time_filter: str = None) -> str:
    """检测价格异常"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## 价格异常检测\n❌ 数据文件不存在"

        df_raw = pd.read_csv(CSV_FILE)
        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        if time_filter:
            df_raw = _apply_time_filter_df(df_raw, time_filter)

        df_valid = df_raw[df_raw['price'] > 0].copy()
        if len(df_valid) < 10:
            return "## 价格异常检测\n❌ 数据不足，需要至少10条记录"

        prices = df_valid['price'].values
        mean_price = np.mean(prices)
        std_price = np.std(prices)

        upper_bound = mean_price + threshold * std_price
        lower_bound = mean_price - threshold * std_price

        anomalies = df_valid[(df_valid['price'] > upper_bound) | (df_valid['price'] < lower_bound)].copy()

        output = f"""## 价格异常检测结果

### 检测参数
- 阈值: {threshold}倍标准差
- 平均价格: {round(mean_price, 2)}
- 正常范围: {round(lower_bound, 2)} - {round(upper_bound, 2)}

### 检测结果
- 发现异常点: {len(anomalies)} 个"""

        if len(anomalies) > 0:
            output += "\n### 异常详情\n| 时间 | 价格 | 偏离倍数 | 成交量 |\n|------|------|----------|--------|"
            for idx, row in anomalies.head(10).iterrows():
                deviation = (row['price'] - mean_price) / std_price
                output += f"\n| {row['datetime'].strftime('%H:%M:%S')} | {row['price']:.2f} | {deviation:.2f}σ | {row['volume']:.6f} |"
            if len(anomalies) > 10:
                output += f"\n... 还有 {len(anomalies) - 10} 个异常点"
        else:
            output += "\n未检测到价格异常，市场表现稳定。"

        output += f"\n\n检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        return output
    except Exception as e:
        return f"## 价格异常检测失败\n❌ 错误: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


# ==================== 工具函数：图表生成 ====================

def generate_price_chart(limit: int = 100, time_filter: str = None) -> str:
    """生成价格走势图"""
    try:
        if not os.path.exists(CSV_FILE):
            return f"## 生成价格走势图\n❌ 数据文件不存在: {CSV_FILE}"

        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "## 生成价格走势图\n❌ CSV文件为空"

        df['datetime'] = pd.to_datetime(df['datetime'])

        if time_filter:
            df = _apply_time_filter_df(df, time_filter)
            if df.empty:
                return f"## 生成价格走势图\n❌ 时间范围 '{time_filter}' 内没有数据"

        df = df.sort_values('timestamp')
        if not time_filter and len(df) > limit:
            df = df.tail(limit).copy()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        ax1.plot(df['datetime'], df['price'], label='价格', color='#2E86AB', linewidth=1.5)
        ax1.set_ylabel('价格 (USDT)', fontsize=12)
        if time_filter:
            ax1.set_title(f'BTC/USDT 价格走势 ({time_filter}, 共{len(df)}条记录)', fontsize=14)
        else:
            ax1.set_title(f'BTC/USDT 价格走势 (最近{len(df)}条记录)', fontsize=14)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)

        ax2.bar(df['datetime'], df['volume'], label='成交量', color='#A23B72', alpha=0.6)
        ax2.set_ylabel('成交量 (BTC)', fontsize=12)
        ax2.set_xlabel('时间', fontsize=12)
        ax2.set_title('成交量分布', fontsize=14)
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)

        plt.tight_layout()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"btc_price_chart_{timestamp}.png"
        chart_path = os.path.join(CHART_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        from drsai.utils.utils import upload_to_hepai_filesystem
        file_obj = upload_to_hepai_filesystem(chart_path)
        preview_url = file_obj.get("url", "")

        # 计算价格统计
        price_min = df['price'].min()
        price_max = df['price'].max()
        price_avg = df['price'].mean()

        # 返回Markdown格式，可直接展示
        return f"""## BTC价格走势图

![价格走势图]({preview_url})

**数据记录数:** {len(df)}条
**时间范围:** {time_filter if time_filter else '最近数据'}

### 价格统计
- 最高价: {price_min:.2f} USDT
- 最低价: {price_max:.2f} USDT
- 平均价: {price_avg:.2f} USDT

生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 图表生成失败\n错误信息: {str(e)}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def generate_volume_distribution_chart(limit: int = 100, time_filter: str = None) -> str:
    """生成成交量分布图"""
    try:
        if not os.path.exists(CSV_FILE):
            return f"## 生成成交量分布图\n❌ 数据文件不存在: {CSV_FILE}"

        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "## 生成成交量分布图\n❌ CSV文件为空"

        df['datetime'] = pd.to_datetime(df['datetime'])

        if time_filter:
            df = _apply_time_filter_df(df, time_filter)
            if df.empty:
                return f"## 生成成交量分布图\n❌ 时间范围 '{time_filter}' 内没有数据"

        if not time_filter and len(df) > limit:
            df = df.tail(limit).copy()

        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        ax.hist(df['volume'], bins=30, color='#A23B72', alpha=0.6, edgecolor='black')
        ax.set_xlabel('成交量 (BTC)', fontsize=12)
        ax.set_ylabel('频次', fontsize=12)
        if time_filter:
            ax.set_title(f'成交量分布直方图 ({time_filter}, 共{len(df)}条记录)', fontsize=14)
        else:
            ax.set_title(f'成交量分布直方图 (最近{len(df)}条记录)', fontsize=14)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"volume_distribution_{timestamp_str}.png"
        chart_path = os.path.join(CHART_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        from drsai.utils.utils import upload_to_hepai_filesystem
        file_obj = upload_to_hepai_filesystem(chart_path)
        preview_url = file_obj.get("url", "")

        return f"""## BTC成交量分布图

![成交量分布图]({preview_url})

**数据记录数:** {len(df)}条
**时间范围:** {time_filter if time_filter else '最近数据'}

生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 图表生成失败\n错误信息: {str(e)}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def get_csv_summary(limit: int = 10, time_filter: str = None) -> str:
    """获取CSV数据摘要"""
    try:
        if not os.path.exists(CSV_FILE):
            return f"## 数据摘要\n数据文件不存在: {CSV_FILE}"

        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "## 数据摘要\nCSV文件为空"

        df['datetime'] = pd.to_datetime(df['datetime'])

        if time_filter:
            df = _apply_time_filter_df(df, time_filter)
            if df.empty:
                return f"## 数据摘要\n时间范围 '{time_filter}' 内没有数据"

        total_records = len(df)

        price_min = df['price'].min()
        price_max = df['price'].max()
        price_avg = df['price'].mean()
        volume_total = df['volume'].sum()

        return f"""## 数据摘要统计

**总记录数:** {total_records}条
**时间范围:** {time_filter if time_filter else '全部数据'}

### 价格统计
- 最低价: {price_min:.2f} USDT
- 最高价: {price_max:.2f} USDT
- 平均价: {price_avg:.2f} USDT

### 成交量统计
- 总成交量: {volume_total:.6f} BTC

统计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except Exception as e:
        return f"## 数据摘要失败\n错误信息: {str(e)}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


# ==================== 工具函数：BTC实时价格 ====================

def get_btc_realtime_data() -> str:
    """获取BTC当前价格（从本地CSV文件读取最新数据）"""
    try:
        if not os.path.exists(CSV_FILE):
            return "## BTC当前价格\n❌ 数据文件不存在，请先启动数据获取服务"

        # 读取CSV文件最后一行
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        if len(lines) <= 1:
            return "## BTC当前价格\n❌ 数据文件为空，请等待数据获取"

        # 解析最后一行 (CSV格式: index,datetime,symbol,price,volume,...)
        last_line = lines[-1].strip()
        parts = last_line.split(',')

        if len(parts) < 5:
            return "## BTC当前价格\n❌ 数据格式错误"

        try:
            # CSV格式: index(0), datetime(1), symbol(2), price(3), volume(4)
            index = parts[0]
            dt_str = parts[1].split('.')[0]  # 去除毫秒部分
            symbol = parts[2]
            price = float(parts[3])
            volume = float(parts[4])

            # 计算数据更新时间差
            try:
                from datetime import datetime
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                time_diff = (datetime.now() - dt).total_seconds()
                time_info = f"（{int(time_diff)}秒前更新）"
            except:
                time_info = f"（更新时间: {dt_str}）"

            return f"""## BTC当前价格

**交易对:** {symbol}
**当前价格:** {price:.2f} USDT
**成交量:** {volume:.6f} BTC
**更新时间:** {dt_str}

{time_info}

查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        except ValueError as e:
            return f"## BTC当前价格\n❌ 数据解析失败: {str(e)}\n\n原始数据: {last_line[:100]}"
    except Exception as e:
        return f"## BTC当前价格\n❌ 读取失败: {str(e)}\n\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def get_market_status(exchange: str = "US") -> str:
    """获取市场状态"""
    try:
        client = finnhub.Client(api_key="d5ssulhr01qmiccbs4qgd5ssulhr01qmiccbs4r0")
        data = client.market_status(exchange=exchange)
        is_open = data.get('isOpen', False)
        status_text = "🟢 开盘交易中" if is_open else "🔴 已收盘"
        return f"""## 市场状态

**交易所:** {exchange}
**状态:** {status_text}

查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    except:
        return f"## 市场状态\n获取失败：无法获取市场状态\n\n查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


# ==================== 创建智能体 ====================

def create_agent(
    thread_id: str = None,
    user_id: str = None,
    db_manager: DatabaseManager = None,
    api_key: str = None
):
    """创建股票监控智能体（单智能体，集成所有功能）"""

    # 模型客户端
    model_client = HepAIChatCompletionClient(
        model="deepseek-ai/deepseek-v3",
        api_key=api_key or os.environ.get("HEPAI_API_KEY"),
        base_url="https://aiapi.ihep.ac.cn/apiv2"
    )

    # 集成所有工具
    all_tools = [
        # === 数据获取管理 ===
        check_data_quality,
        get_data_collection_status,
        start_data_collection,
        stop_data_collection,
        restart_data_collection,
        get_csv_file_info,
        # === 数据分析 ===
        get_basic_stats,
        analyze_trend,
        analyze_volatility,
        analyze_time_distribution,
        compare_time_periods,
        detect_price_anomalies,
        # === 图表生成 ===
        generate_price_chart,
        generate_volume_distribution_chart,
        get_csv_summary,
        # === BTC实时价格 ===
        get_btc_realtime_data,
        get_market_status,
    ]

    return AssistantAgent(
        name="stock_monitor_agent",
        model_client=model_client,
        model_client_stream=True,
        tools=all_tools,
        system_message="""你是股票监控系统智能助手，集成了数据获取管理、数据分析、图表生成和BTC实时价格查询功能。

## 功能分类

### 1. BTC价格查询
- get_btc_realtime_data: 获取BTC实时价格
- get_market_status: 获取市场状态

### 2. 数据获取管理
- check_data_quality: 检查数据质量
- get_data_collection_status: 查询数据获取服务状态
- start_data_collection: 启动数据获取服务
- stop_data_collection: 停止数据获取服务
- restart_data_collection: 重启数据获取服务
- get_csv_file_info: 获取数据文件信息

### 3. 数据分析
- get_basic_stats: 获取基本统计（价格、成交量统计）
- analyze_trend: 分析价格趋势（上涨/下跌/震荡）
- analyze_volatility: 分析价格波动性
- analyze_time_distribution: 按小时统计交易活跃度
- compare_time_periods: 比较两个时间段的数据
- detect_price_anomalies: 检测价格异常

### 4. 图表生成
- generate_price_chart: 生成价格走势图（支持时间过滤）
- generate_volume_distribution_chart: 生成成交量分布图
- get_csv_summary: 获取数据摘要统计

## 工具参数说明
- limit: 最多分析N条记录
- time_filter: 时间过滤表达式，支持：
  * 按分钟: "30分钟" / "30min"
  * 按小时: "过去一小时" / "1h" / "2h" / "6h" / "12h" / "24h"
  * 按时段: "今天上午" / "上午" / "今天下午" / "下午"
  * 按天: "今天" / "今日" / "昨天"
  * 具体时间段: "下午五点到六点" / "17点到18点"
- threshold: 异常检测阈值（标准差倍数），默认2.0

## 工作流程
1. 理解用户需求
2. 选择合适的工具
3. 调用工具并获取结果
4. 用简洁清晰的语言向用户展示结果   

## 重要提示
- 图表生成成功后，使用Markdown格式展示: ![图表](preview_url)
- 所有报告必须包含系统时间
- 用简洁的中文回复，不要提及"工具"、"JSON"、"调用"等技术术语
- 提供数据驱动的洞察和建议
""",
        tool_call_summary_prompt="""请用简洁的中文回复用户。

重要规则：
1. 如果工具结果包含Markdown格式（如![图片](url)、表格、统计数据等），必须**完全保留**原始的Markdown格式，不要修改
2. 特别是图片链接，必须原样保留 ![图片](url) 格式
3. 在保留原始内容的基础上，可以在开头或结尾添加简短的中文确认语句
4. 不要解释或总结工具已经格式化好的内容

示例回复格式：
"好的，[任务已完成]

[工具返回的完整Markdown内容，原样保留]"
""",
        reflect_on_tool_use=False,
        thread_id=thread_id,
        db_manager=db_manager,
        user_id=user_id,
    )


# ==================== 运行模式 ====================

async def run_console_mode():
    """运行Agent（命令行模式）"""
    print("="*60) 
    print("=== 股票监控智能体系统（命令行模式）===")
    print("="*60 + "\n")

    task = "查看数据获取状态"
    await run_console(agent_factory=create_agent, task=task)


async def run_backend_service():
    """启动后端API服务"""
    await run_worker(
        agent_name="stock_monitor_agent",
        author="lin@ihep.ac.cn",
        permission='groups: drsai; users: admin, lin@ihep.ac.cn; owner: lin@ihep.ac.cn',
        description="股票监控智能体系统：集成BTC价格查询、数据分析、图表生成、数据获取管理等功能。",
        version="3.0.0",
        logo="https://aiapi.ihep.ac.cn/apiv2/files/file-8572b27d093f4e15913bebfac3645e20/preview",
        examples=[
            "分析今天的价格趋势",
            "生成价格走势图",
            "画一下过去一小时的价格图表",
            "分析价格波动性",
            "启动数据获取服务",
            "查看数据获取状态",
            "生成成交量分布图",
            "比较今天和昨天的交易情况",
            "检测价格异常波动",
            "按小时统计交易活跃度",
            "给我一个全面的数据分析报告",
        ],
        agent_factory=create_agent,
        host="0.0.0.0",
        port=42820,
        no_register=False,
        enable_openwebui_pipeline=True,
        history_mode="backend",
        use_api_key_mode="backend",
    )


if __name__ == "__main__":
    if DEBUG_MODE:
        asyncio.run(run_console_mode())
    else:
        asyncio.run(run_backend_service())
