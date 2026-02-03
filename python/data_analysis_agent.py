# -*- coding: utf-8 -*-
"""
数据分析Agent - 对BTC交易数据进行基本分析
数据源: /data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

# 添加项目路径
sys.path.insert(0, '/data/juno/lin/agent/drsai-main')

# ==================== 配置 ====================
DEBUG_MODE = True  # True=命令行模式, False=后端服务模式

CSV_FILE = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

# ==================== 数据处理函数 ====================

def _load_data(time_filter: str = None, limit: int = None):
    """加载数据并根据条件过滤"""
    if not os.path.exists(CSV_FILE):
        return None

    df = pd.read_csv(CSV_FILE)
    if df.empty:
        return None

    df['datetime'] = pd.to_datetime(df['datetime'])

    # 过滤掉占位数据（价格为0的记录）
    df = df[df['price'] > 0].copy()

    # 应用时间过滤（如果提供了具体的分析函数）
    if time_filter:
        df = _apply_time_filter(df, time_filter)

    # 限制记录数
    if limit and len(df) > limit:
        df = df.tail(limit).copy()

    return df.sort_values('timestamp')


def _apply_time_filter(df: pd.DataFrame, time_filter: str) -> pd.DataFrame:
    """应用时间过滤"""
    import re
    now = datetime.now()
    time_filter_original = time_filter.strip()
    time_filter = time_filter_original.lower()

    # 按分钟过滤 (优先处理，避免与小时冲突)
    if 'min' in time_filter or '分钟' in time_filter:
        # 阿拉伯数字: "5分钟" / "5min"
        match = re.search(r'(\d+)\s*(min|分钟)', time_filter)
        if match:
            mins = int(match.group(1))
            start = now - timedelta(minutes=mins)
            return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        # 中文数字: "五分钟"、"十分钟"、"三十分钟"
        cn_min_map = {'十': 10, '五': 5, '三': 3, '两': 2, '二': 2, '一': 1, '六': 6, '十五': 15, '二十': 20, '三十': 30, '六十': 60}
        for cn_num, mins in cn_min_map.items():
            if cn_num in time_filter and '分钟' in time_filter:
                start = now - timedelta(minutes=mins)
                return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()

    # 按小时过滤
    if '过去一小时' in time_filter or '1h' in time_filter or '1小时' in time_filter:
        start = now - timedelta(hours=1)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
    elif '2h' in time_filter or '2小时' in time_filter:
        start = now - timedelta(hours=2)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
    elif '3h' in time_filter or '3小时' in time_filter:
        start = now - timedelta(hours=3)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
    elif '6h' in time_filter or '6小时' in time_filter:
        start = now - timedelta(hours=6)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
    elif '12h' in time_filter or '12小时' in time_filter:
        start = now - timedelta(hours=12)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
    elif '24h' in time_filter or '24小时' in time_filter:
        start = now - timedelta(hours=24)
        return df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()

    # 按天过滤
    if '今天' in time_filter or '今日' in time_filter:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        filtered = df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        # 如果今天没有数据，自动使用最近24小时的数据
        if filtered.empty:
            start = now - timedelta(hours=24)
            filtered = df[(df['datetime'] >= start) & (df['datetime'] <= now)].copy()
        return filtered
    elif '昨天' in time_filter and '点' not in time_filter:
        # 只有"昨天"没有具体小时时才返回全天数据
        yesterday = now - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()

    # 具体时间段解析 (如 "昨天下午六点" / "昨天18点" / "下午5点到6点")
    cn_hour_map = {
        '零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4,
        '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
        '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '二十一': 21, '二十二': 22, '二十三': 23
    }

    # 判断日期
    day_offset = 0
    if '昨天' in time_filter:
        day_offset = -1
    elif '前天' in time_filter:
        day_offset = -2
    elif '明天' in time_filter:
        day_offset = 1

    # 判断上午/下午
    is_afternoon = '下午' in time_filter
    is_morning = '上午' in time_filter or '凌晨' in time_filter or '早上' in time_filter

    # 匹配阿拉伯数字小时 (优先匹配范围，再匹配单点)
    import re
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

    # 匹配单点小时 (如 "昨天下午六点", "昨天18点")
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

    # 匹配中文数字小时 (如 "昨天下午六点")
    for cn_num, hour in cn_hour_map.items():
        if cn_num in time_filter and '点' in time_filter:
            # 检查是否是范围 (如 "六点到七点")
            for cn_num2, hour2 in cn_hour_map.items():
                if cn_num2 in time_filter and cn_num2 != cn_num:
                    if is_afternoon and hour < 12:
                        hour += 12
                    if is_afternoon and hour2 < 12:
                        hour2 += 12
                    base_date = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
                    start = base_date + timedelta(hours=hour)
                    end = base_date + timedelta(hours=hour2)
                    return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()
            # 单个小时
            if is_afternoon and hour < 12:
                hour += 12
            elif is_morning and hour == 12:
                hour = 0
            base_date = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
            start = base_date + timedelta(hours=hour)
            end = base_date + timedelta(hours=hour+1)
            return df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()

    return df


# ==================== 分析工具函数 ====================

def get_basic_stats(limit: int = 1000, time_filter: str = None) -> str:
    """
    获取基本统计数据：价格、成交量的统计信息

    Args:
        limit: 最多分析N条记录
        time_filter: 时间过滤，如"今天"、"过去一小时"、"过去十分钟"

    Returns:
        JSON格式统计结果
    """
    try:
        # 先加载数据不过滤price=0，用于检查是否有占位数据
        df_raw = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else None
        if df_raw is None or df_raw.empty:
            return json.dumps({"status": "ERROR", "message": "数据文件不存在或为空"}, ensure_ascii=False)

        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        # 应用时间过滤（不过滤price=0）
        if time_filter:
            df_raw = _apply_time_filter(df_raw, time_filter)

        if df_raw.empty:
            return json.dumps({"status": "ERROR", "message": f"时间范围 '{time_filter}' 内没有数据"}, ensure_ascii=False)

        # 检查是否有有效数据
        df_valid = df_raw[df_raw['price'] > 0].copy()

        if df_valid.empty:
            # 全是占位数据
            latest_time = df_raw['datetime'].max()
            now = datetime.now()
            gap_minutes = (now - latest_time).total_seconds() / 60
            return json.dumps({
                "status": "ERROR",
                "message": f"所选时间范围内没有有效交易数据（全是网络断开时的占位数据）。最近数据时间是 {latest_time.strftime('%H:%M:%S')}，距今约 {gap_minutes:.0f} 分钟。建议使用更长时间范围，如'过去一小时'。"
            }, ensure_ascii=False)

        # 价格统计
        price_min = df_valid['price'].min()
        price_max = df_valid['price'].max()
        price_mean = df_valid['price'].mean()
        price_median = df_valid['price'].median()
        price_std = df_valid['price'].std()
        price_range = price_max - price_min

        # 成交量统计
        volume_mean = df_valid['volume'].mean()
        volume_total = df_valid['volume'].sum()
        volume_max = df_valid['volume'].max()

        # 数据点数量
        total_records = len(df_raw)
        valid_records = len(df_valid)
        placeholder_records = total_records - valid_records

        # 时间范围
        time_start = df_raw['datetime'].min()
        time_end = df_raw['datetime'].max()
        time_span = (time_end - time_start).total_seconds() / 60  # 分钟

        # 计算价格变化
        first_price = df_valid.iloc[0]['price']
        last_price = df_valid.iloc[-1]['price']
        price_change = last_price - first_price
        price_change_pct = (price_change / first_price) * 100

        # 返回自然语言格式
        change_symbol = "+" if price_change >= 0 else ""
        return f"""## 基本统计结果

### 数据概况
- 分析记录数: {valid_records} 条
- 时间范围: {time_start.strftime("%H:%M:%S")} - {time_end.strftime("%H:%M:%S")} (约 {round(time_span, 1)} 分钟)

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
- 最大成交量: {round(volume_max, 6)}"""

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": f"分析失败: {str(e)}"}, ensure_ascii=False)


def analyze_trend(limit: int = 1000, time_filter: str = None) -> str:
    """
    分析价格趋势：上涨、下跌、震荡

    Args:
        limit: 最多分析N条记录
        time_filter: 时间过滤，如"过去一小时"、"今天"、"昨天下午六点"

    Returns:
        JSON格式趋势分析结果
    """
    try:
        # 先加载数据不过滤price=0
        df_raw = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else None
        if df_raw is None or df_raw.empty:
            return json.dumps({"status": "ERROR", "message": "数据文件不存在或为空"}, ensure_ascii=False)

        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        # 应用时间过滤
        if time_filter:
            df_raw = _apply_time_filter(df_raw, time_filter)

        if df_raw.empty:
            return json.dumps({"status": "ERROR", "message": f"时间范围 '{time_filter}' 内没有数据"}, ensure_ascii=False)

        # 检查是否有有效数据
        df_valid = df_raw[df_raw['price'] > 0].copy()

        if df_valid.empty:
            if len(df_raw) > 0:
                latest_time = df_raw['datetime'].max()
                now = datetime.now()
                gap_minutes = (now - latest_time).total_seconds() / 60
                return json.dumps({
                    "status": "ERROR",
                    "message": f"所选时间范围内没有有效交易数据（全是占位数据）。最近数据时间是 {latest_time.strftime('%H:%M:%S')}，距今约 {gap_minutes:.0f} 分钟。建议使用更长时间范围。"
                }, ensure_ascii=False)
        if len(df_valid) < 2:
            return json.dumps({"status": "ERROR", "message": "数据不足，无法分析趋势（需要至少2条有效记录）"}, ensure_ascii=False)

        # 计算价格变化
        prices = df_valid['price'].values
        first_price = prices[0]
        last_price = prices[-1]
        total_change = last_price - first_price
        total_change_pct = (total_change / first_price) * 100

        # 计算移动平均
        window = min(20, len(df_valid) // 4) if len(df_valid) >= 8 else 2
        df_valid['ma'] = df_valid['price'].rolling(window=window).mean()

        # 判断趋势
        trend = "震荡"
        trend_strength = "弱"
        direction = "中性"

        if total_change_pct > 0.5:
            trend = "上涨"
            direction = "上升"
            if total_change_pct > 2:
                trend_strength = "强"
            elif total_change_pct > 1:
                trend_strength = "中"
        elif total_change_pct < -0.5:
            trend = "下跌"
            direction = "下降"
            if total_change_pct < -2:
                trend_strength = "强"
            elif total_change_pct < -1:
                trend_strength = "中"

        # 计算波动点
        up_moves = 0
        down_moves = 0
        for i in range(1, len(prices)):
            if prices[i] > prices[i-1]:
                up_moves += 1
            elif prices[i] < prices[i-1]:
                down_moves += 1

        total_moves = up_moves + down_moves
        up_ratio = (up_moves / total_moves * 100) if total_moves > 0 else 50

        # 计算最高点和最低点
        max_price_idx = df_valid['price'].idxmax()
        min_price_idx = df_valid['price'].idxmin()
        max_price_time = df_valid.loc[max_price_idx, 'datetime']
        min_price_time = df_valid.loc[min_price_idx, 'datetime']

        change_symbol = "+" if total_change >= 0 else ""
        return f"""## 趋势分析结果

### 总体趋势
- 趋势方向: **{trend}** ({trend_strength})
- 价格变化: {round(first_price, 2)} → {round(last_price, 2)} ({change_symbol}{round(total_change, 2)} / {change_symbol}{round(total_change_pct, 3)}%)

### 波动分析
- 上涨次数: {up_moves}
- 下跌次数: {down_moves}
- 上涨占比: {round(up_ratio, 1)}%

### 极值点
- 最高价: {round(df_valid['price'].max(), 2)} ({max_price_time.strftime("%H:%M:%S")})
- 最低价: {round(df_valid['price'].min(), 2)} ({min_price_time.strftime("%H:%M:%S")})

### 总结
共分析 {len(df_valid)} 条记录，价格呈{direction}趋势，波动强度为{trend_strength}。"""

    except Exception as e:
        return f"## 趋势分析失败\n错误信息: {str(e)}"


def analyze_volatility(limit: int = 1000, time_filter: str = None) -> str:
    """
    分析价格波动性

    Args:
        limit: 最多分析N条记录
        time_filter: 时间过滤，如"过去一小时"、"昨天下午六点"

    Returns:
        JSON格式波动性分析结果
    """
    try:
        # 先加载数据不过滤price=0
        df_raw = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else None
        if df_raw is None or df_raw.empty:
            return json.dumps({"status": "ERROR", "message": "数据文件不存在或为空"}, ensure_ascii=False)

        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        # 应用时间过滤
        if time_filter:
            df_raw = _apply_time_filter(df_raw, time_filter)

        if df_raw.empty:
            return json.dumps({"status": "ERROR", "message": f"时间范围 '{time_filter}' 内没有数据"}, ensure_ascii=False)

        # 检查是否有有效数据
        df_valid = df_raw[df_raw['price'] > 0].copy()

        if df_valid.empty:
            if len(df_raw) > 0:
                latest_time = df_raw['datetime'].max()
                now = datetime.now()
                gap_minutes = (now - latest_time).total_seconds() / 60
                return json.dumps({
                    "status": "ERROR",
                    "message": f"所选时间范围内没有有效交易数据（全是占位数据）。最近数据时间是 {latest_time.strftime('%H:%M:%S')}，距今约 {gap_minutes:.0f} 分钟。建议使用更长时间范围。"
                }, ensure_ascii=False)
        if len(df_valid) < 2:
            return json.dumps({"status": "ERROR", "message": "数据不足，无法分析波动性"}, ensure_ascii=False)

        prices = df_valid['price'].values

        # 基本统计
        mean_price = np.mean(prices)
        std_dev = np.std(prices)
        variance = np.var(prices)

        # 变异系数 (CV = std / mean * 100)
        cv = (std_dev / mean_price) * 100 if mean_price > 0 else 0

        # 计算价格变化幅度
        price_changes = []
        for i in range(1, len(prices)):
            change_pct = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
            price_changes.append(abs(change_pct))

        avg_change = np.mean(price_changes) if price_changes else 0
        max_change = np.max(price_changes) if price_changes else 0

        # 波动性等级判断
        volatility_level = "低"
        if cv > 1:
            volatility_level = "极高"
        elif cv > 0.5:
            volatility_level = "高"
        elif cv > 0.2:
            volatility_level = "中"

        # 计算真实波动幅度 (基于最高最低价)
        true_range = df_valid['price'].max() - df_valid['price'].min()
        true_range_pct = (true_range / df_valid['price'].min()) * 100

        result = {
            "status": "SUCCESS",
            "message": "波动性分析完成",
            "data": {
                "volatility_level": volatility_level,
                "statistics": {
                    "std_dev": round(std_dev, 2),
                    "variance": round(variance, 2),
                    "cv": round(cv, 4),  # 变异系数
                    "mean_price": round(mean_price, 2)
                },
                "price_changes": {
                    "avg_change_pct": round(avg_change, 4),
                    "max_change_pct": round(max_change, 4)
                },
                "true_range": {
                    "absolute": round(true_range, 2),
                    "percent": round(true_range_pct, 4)
                },
                "records_analyzed": len(df_valid)
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return f"""## 波动性分析结果

### 波动等级
- 波动水平: **{volatility_level}**

### 统计指标
- 标准差: {round(std_dev, 2)}
- 方差: {round(variance, 2)}
- 变异系数: {round(cv, 4)}%
- 平均价格: {round(mean_price, 2)}

### 价格变化幅度
- 平均变化幅度: {round(avg_change, 4)}%
- 最大变化幅度: {round(max_change, 4)}%

### 真实波动幅度
- 绝对幅度: {round(true_range, 2)}
- 相对幅度: {round(true_range_pct, 4)}%

### 风险评估
当前波动性等级为 **{volatility_level}** {'(高风险)' if volatility_level in ['高', '极高'] else '(相对稳定)'}。"""

    except Exception as e:
        return f"## 波动性分析失败\n错误信息: {str(e)}"


def analyze_time_distribution(limit: int = 10000, time_filter: str = None) -> str:
    """
    分析时间分布：按小时统计交易活跃度

    Args:
        limit: 最多分析N条记录
        time_filter: 时间过滤

    Returns:
        JSON格式时间分布分析结果
    """
    try:
        df = _load_data(time_filter=time_filter, limit=limit)
        if df is None or df.empty:
            return json.dumps({"status": "ERROR", "message": "没有可用数据"}, ensure_ascii=False)

        df_valid = df[df['price'] > 0].copy()
        if df_valid.empty:
            return json.dumps({"status": "ERROR", "message": "没有有效数据"}, ensure_ascii=False)

        # 添加小时列
        df_valid['hour'] = df_valid['datetime'].dt.hour
        df_valid['date'] = df_valid['datetime'].dt.date

        # 按小时统计
        hourly_stats = df_valid.groupby('hour').agg({
            'price': ['mean', 'min', 'max', 'count'],
            'volume': 'sum'
        }).round(2)

        hourly_stats.columns = ['avg_price', 'min_price', 'max_price', 'trade_count', 'total_volume']

        # 转换为字典
        hourly_data = {}
        for hour, row in hourly_stats.iterrows():
            hourly_data[str(hour).zfill(2) + ":00"] = {
                "avg_price": float(row['avg_price']),
                "min_price": float(row['min_price']),
                "max_price": float(row['max_price']),
                "trade_count": int(row['trade_count']),
                "total_volume": round(float(row['total_volume']), 6)
            }

        # 找出最活跃和最不活跃的小时
        most_active = hourly_stats['trade_count'].idxmax()
        least_active = hourly_stats['trade_count'].idxmin()

        # 按日期统计
        daily_stats = df_valid.groupby('date').agg({
            'price': ['mean', 'min', 'max'],
            'volume': 'sum'
        }).round(2)

        daily_data = {}
        for date, row in daily_stats.iterrows():
            daily_data[str(date)] = {
                "avg_price": float(row[('price', 'mean')]),
                "min_price": float(row[('price', 'min')]),
                "max_price": float(row[('price', 'max')]),
                "total_volume": round(float(row[('volume', 'sum')]), 6)
            }

        result = {
            "status": "SUCCESS",
            "message": "时间分布分析完成",
            "data": {
                "hourly": hourly_data,
                "daily": daily_data,
                "summary": {
                    "most_active_hour": f"{most_active}:00",
                    "least_active_hour": f"{least_active}:00",
                    "hours_with_data": len(hourly_data)
                },
                "records_analyzed": len(df_valid)
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # 构建自然语言输出
        output = f"""## 时间分布分析结果

### 概述
- 覆盖小时数: {len(hourly_data)}
- 最活跃时段: {most_active}:00
- 最不活跃时段: {least_active}:00

### 按小时统计
| 时段 | 平均价 | 最低价 | 最高价 | 交易次数 | 总成交量 |
|------|--------|--------|--------|----------|----------|
"""
        # 只显示前5个最活跃的小时
        top_hours = sorted(hourly_data.items(), key=lambda x: x[1]['trade_count'], reverse=True)[:5]
        for hour, data in top_hours:
            output += f"| {hour} | {data['avg_price']:.2f} | {data['min_price']:.2f} | {data['max_price']:.2f} | {data['trade_count']} | {data['total_volume']:.4f} |\n"

        output += f"\n### 按日期统计\n"
        for date, data in daily_data.items():
            output += f"- **{date}**: 均价 {data['avg_price']:.2f}, 范围 {data['min_price']:.2f}-{data['max_price']:.2f}, 成交量 {data['total_volume']:.4f}\n"

        return output

    except Exception as e:
        return f"## 时间分布分析失败\n错误信息: {str(e)}"


def compare_time_periods(period1: str = "过去一小时", period2: str = "过去一小时前一小时") -> str:
    """
    比较两个时间段的数据

    Args:
        period1: 第一个时间段描述
        period2: 第二个时间段描述

    Returns:
        JSON格式比较结果
    """
    try:
        now = datetime.now()

        # 解析时间段
        def parse_period(p: str):
            p = p.lower().strip()
            if '过去一小时' in p or '1h' in p or '1小时' in p:
                return now - timedelta(hours=1), now
            elif '过去两小时' in p or '2h' in p or '2小时' in p:
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
            return json.dumps({"status": "ERROR", "message": "无法解析时间段"}, ensure_ascii=False)

        # 加载两个时间段的数据
        df = _load_data(limit=None)
        if df is None or df.empty:
            return json.dumps({"status": "ERROR", "message": "没有可用数据"}, ensure_ascii=False)

        df1 = df[(df['datetime'] >= start1) & (df['datetime'] <= end1)].copy()
        df2 = df[(df['datetime'] >= start2) & (df['datetime'] <= end2)].copy()

        df1 = df1[df1['price'] > 0]
        df2 = df2[df2['price'] > 0]

        if df1.empty or df2.empty:
            return json.dumps({"status": "ERROR", "message": "某个时间段没有数据"}, ensure_ascii=False)

        # 统计比较
        stats1 = {
            "avg_price": df1['price'].mean(),
            "min_price": df1['price'].min(),
            "max_price": df1['price'].max(),
            "volume": df1['volume'].sum(),
            "records": len(df1)
        }

        stats2 = {
            "avg_price": df2['price'].mean(),
            "min_price": df2['price'].min(),
            "max_price": df2['price'].max(),
            "volume": df2['volume'].sum(),
            "records": len(df2)
        }

        # 计算差异
        comparison = {
            "avg_price_change": stats1['avg_price'] - stats2['avg_price'],
            "avg_price_change_pct": ((stats1['avg_price'] - stats2['avg_price']) / stats2['avg_price'] * 100) if stats2['avg_price'] > 0 else 0,
            "volume_change": stats1['volume'] - stats2['volume'],
            "volume_change_pct": ((stats1['volume'] - stats2['volume']) / stats2['volume'] * 100) if stats2['volume'] > 0 else 0
        }

        result = {
            "status": "SUCCESS",
            "message": "时间段比较完成",
            "data": {
                "period1": {
                    "name": period1,
                    "start": start1.strftime("%Y-%m-%d %H:%M:%S"),
                    "end": end1.strftime("%Y-%m-%d %H:%M:%S"),
                    "stats": {k: round(v, 4) if isinstance(v, float) else v for k, v in stats1.items()}
                },
                "period2": {
                    "name": period2,
                    "start": start2.strftime("%Y-%m-%d %H:%M:%S"),
                    "end": end2.strftime("%Y-%m-%d %H:%M:%S"),
                    "stats": {k: round(v, 4) if isinstance(v, float) else v for k, v in stats2.items()}
                },
                "comparison": {
                    "avg_price_change": round(comparison['avg_price_change'], 2),
                    "avg_price_change_pct": round(comparison['avg_price_change_pct'], 4),
                    "volume_change": round(comparison['volume_change'], 6),
                    "volume_change_pct": round(comparison['volume_change_pct'], 4)
                }
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # 返回自然语言格式
        price_change_symbol = "+" if comparison['avg_price_change'] >= 0 else ""
        volume_change_symbol = "+" if comparison['volume_change'] >= 0 else ""
        return f"""## 时间段比较结果

### 时间段1: {period1}
- 时间: {start1.strftime("%m-%d %H:%M")} - {end1.strftime("%m-%d %H:%M")}
- 平均价: {round(stats1['avg_price'], 2)}
- 价格范围: {round(stats1['min_price'], 2)} - {round(stats1['max_price'], 2)}
- 总成交量: {round(stats1['volume'], 6)}
- 记录数: {stats1['records']}

### 时间段2: {period2}
- 时间: {start2.strftime("%m-%d %H:%M")} - {end2.strftime("%m-%d %H:%M")}
- 平均价: {round(stats2['avg_price'], 2)}
- 价格范围: {round(stats2['min_price'], 2)} - {round(stats2['max_price'], 2)}
- 总成交量: {round(stats2['volume'], 6)}
- 记录数: {stats2['records']}

### 差异分析
- 平均价变化: {price_change_symbol}{round(comparison['avg_price_change'], 2)} ({price_change_symbol}{round(comparison['avg_price_change_pct'], 3)}%)
- 成交量变化: {volume_change_symbol}{round(comparison['volume_change'], 6)} ({volume_change_symbol}{round(comparison['volume_change_pct'], 3)}%)"""

    except Exception as e:
        return f"## 时间段比较失败\n错误信息: {str(e)}"


def detect_price_anomalies(limit: int = 1000, threshold: float = 2.0, time_filter: str = None) -> str:
    """
    检测价格异常（使用标准差方法）

    Args:
        limit: 最多分析N条记录
        threshold: 异常阈值（标准差倍数），默认2倍标准差
        time_filter: 时间过滤，如"过去一小时"、"昨天下午六点"

    Returns:
        JSON格式异常检测结果
    """
    try:
        # 先加载数据不过滤price=0
        df_raw = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else None
        if df_raw is None or df_raw.empty:
            return json.dumps({"status": "ERROR", "message": "数据文件不存在或为空"}, ensure_ascii=False)

        df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

        # 应用时间过滤
        if time_filter:
            df_raw = _apply_time_filter(df_raw, time_filter)

        if df_raw.empty:
            return json.dumps({"status": "ERROR", "message": f"时间范围 '{time_filter}' 内没有数据"}, ensure_ascii=False)

        # 检查是否有有效数据
        df_valid = df_raw[df_raw['price'] > 0].copy()

        if df_valid.empty:
            if len(df_raw) > 0:
                latest_time = df_raw['datetime'].max()
                now = datetime.now()
                gap_minutes = (now - latest_time).total_seconds() / 60
                return json.dumps({
                    "status": "ERROR",
                    "message": f"所选时间范围内没有有效交易数据。最近数据时间是 {latest_time.strftime('%H:%M:%S')}，距今约 {gap_minutes:.0f} 分钟。"
                }, ensure_ascii=False)
        if len(df_valid) < 10:
            return json.dumps({"status": "ERROR", "message": "数据不足，无法检测异常（需要至少10条记录）"}, ensure_ascii=False)

        prices = df_valid['price'].values
        mean_price = np.mean(prices)
        std_price = np.std(prices)

        # 检测异常值（超过阈值倍标准差）
        upper_bound = mean_price + threshold * std_price
        lower_bound = mean_price - threshold * std_price

        anomalies = df_valid[(df_valid['price'] > upper_bound) | (df_valid['price'] < lower_bound)].copy()

        anomaly_list = []
        for idx, row in anomalies.iterrows():
            anomaly_list.append({
                "datetime": row['datetime'].strftime("%Y-%m-%d %H:%M:%S"),
                "price": round(row['price'], 2),
                "volume": round(row['volume'], 6),
                "deviation": round((row['price'] - mean_price) / std_price, 2)
            })

        result = {
            "status": "SUCCESS",
            "message": "异常检测完成",
            "data": {
                "threshold": threshold,
                "bounds": {
                    "upper": round(upper_bound, 2),
                    "lower": round(lower_bound, 2),
                    "mean": round(mean_price, 2)
                },
                "anomalies": {
                    "count": len(anomalies),
                    "details": anomaly_list[:20]  # 最多返回20条
                },
                "records_analyzed": len(df_valid)
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # 返回自然语言格式
        output = f"""## 价格异常检测结果

### 检测参数
- 阈值: {threshold}倍标准差
- 平均价格: {round(mean_price, 2)}
- 正常范围: {round(lower_bound, 2)} - {round(upper_bound, 2)}
- 分析记录数: {len(df_valid)}

### 检测结果
- 发现异常点: {len(anomalies)} 个
"""
        if len(anomalies) > 0:
            output += "\n### 异常详情\n| 时间 | 价格 | 偏离倍数 | 成交量 |\n|------|------|----------|--------|\n"
            for a in anomaly_list[:10]:  # 只显示前10个
                output += f"| {a['datetime']} | {a['price']:.2f} | {a['deviation']:.2f}σ | {a['volume']:.6f} |\n"
            if len(anomalies) > 10:
                output += f"\n... 还有 {len(anomalies) - 10} 个异常点\n"
        else:
            output += "\n未检测到价格异常，市场表现稳定。"

        return output

    except Exception as e:
        return f"## 异常检测失败\n错误信息: {str(e)}"


# ==================== 智能体系统 ====================
from drsai import AssistantAgent, HepAIChatCompletionClient, Console, run_worker, run_console
from drsai.modules.managers.database import DatabaseManager


def create_analysis_agent(
    api_key: str = None,
    thread_id: str = None,
    user_id: str = None,
    db_manager: DatabaseManager = None
):
    """创建数据分析Agent（工厂函数）"""
    model_client = HepAIChatCompletionClient(
        model="deepseek-ai/deepseek-v3",
        api_key=api_key or os.environ.get("HEPAI_API_KEY"),
        base_url="https://aiapi.ihep.ac.cn/apiv2"
    )

    return AssistantAgent(
        name="data_analysis_agent",
        model_client=model_client,
        tools=[
            get_basic_stats,
            analyze_trend,
            analyze_volatility,
            analyze_time_distribution,
            compare_time_periods,
            detect_price_anomalies
        ],
        system_message="""你是一个专业的数据分析助手，专门分析BTC交易数据。

你的能力：
1. 基本统计 - 价格、成交量的统计信息（最高、最低、平均、波动等）
2. 趋势分析 - 判断价格趋势（上涨/下跌/震荡）及强度
3. 波动性分析 - 分析价格波动程度和风险
4. 时间分布 - 按小时/天统计交易活跃度
5. 时间段比较 - 对比两个时间段的交易情况
6. 异常检测 - 检测价格异常波动

工具说明：
- get_basic_stats: 获取基本统计，可指定时间范围（time_filter="今天"/"过去一小时"）
- analyze_trend: 分析价格趋势方向和强度
- analyze_volatility: 分析价格波动性等级（低/中/高/极高）
- analyze_time_distribution: 按小时统计交易活跃度
- compare_time_periods: 比较两个时间段（如"过去一小时"和"过去两小时"）
- detect_price_anomalies: 检测价格异常，可指定阈值（threshold标准差倍数）

工作流程：
- 理解用户分析需求
- 调用合适的分析工具
- 用清晰易懂的语言向用户解释分析结果
- 提供数据驱动的洞察和建议

重要：
- 用简洁专业的语言报告结果
- 解释数据含义，不只是罗列数字
- 提供有价值的分析和建议
- 包含系统时间
- 工具返回的是JSON格式，需要解读后用自然语言展示""",
        reflect_on_tool_use=False,
        tool_call_summary_format="调用工具 {tool_name}，参数: {arguments}\n结果: {result}\n",
    )


# ==================== 启动模式 ====================
async def run_console_mode():
    """启动命令行交互模式"""
    agent = create_analysis_agent()
    await Console(agent.run_stream(task="分析最近一分钟的数据波动水平"))


async def run_backend_service():
    """启动后端API服务"""
    await run_worker(
        agent_name="data_analysis_agent",
        author="lin@ihep.ac.cn",
        permission='groups: drsai; users: admin, lin@ihep.ac.cn; owner: lin@ihep.ac.cn',
        description="数据分析Agent：对BTC交易数据进行基本统计分析，包括趋势分析、波动性分析、时间分布分析、异常检测等。",
        version="1.0.0",
        examples=[
            "分析今天的价格趋势",
            "统计过去一小时的基本数据",
            "分析价格波动性",
            "按小时统计交易活跃度",
            "比较今天和昨天的交易情况",
            "检测价格异常波动",
            "给我一个全面的数据分析报告",
        ],
        agent_factory=create_analysis_agent,
        host="0.0.0.0",
        port=42843,
        no_register=False,
        enable_openwebui_pipeline=True,
        history_mode="backend",
        use_api_key_mode="backend",
    )


if __name__ == "__main__":
    import asyncio

    if DEBUG_MODE:
        asyncio.run(run_console_mode())
    else:
        asyncio.run(run_backend_service())
