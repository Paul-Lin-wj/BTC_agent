# 数据可视化Agent
# 读取CSV数据并绘制图表，上传到DDF文件系统并返回预览链接

import sys
import os
sys.path.insert(0, '/data/juno/lin/agent/drsai-main')

import asyncio
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from datetime import datetime
from pathlib import Path

# 设置matplotlib使用非GUI后端
matplotlib.use('Agg')

# 配置中文字体 - 使用系统中可用的中文字体
# 检查可用字体
import matplotlib.font_manager as fm
available_fonts = set([f.name for f in fm.fontManager.ttflist])

# 优先使用系统中存在的中文字体 (CJK字体支持中日韩)
preferred_fonts = [
    'Noto Sans CJK SC',     # 思源黑体简体中文
    'Noto Sans CJK JP',     # 思源黑体日文(支持中文)
    'Noto Serif CJK JP',    # 思源宋体日文(支持中文)
    'WenQuanYi Zen Hei Sharp',  # 文泉驛點陣正黑
    'AR PL UMing CN',       # AR PL 明体
    'SimSun',               # 宋体
    'SimHei',               # 黑体
]

# 查找第一个可用的中文字体
selected_font = None
for font in preferred_fonts:
    if font in available_fonts:
        selected_font = font
        break

# 设置字体，如果都不可用则使用DejaVu Sans作为后备
if selected_font:
    plt.rcParams['font.sans-serif'] = [selected_font, 'DejaVu Sans']
else:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans']

plt.rcParams['axes.unicode_minus'] = False

# ==================== 配置 ====================
# DEBUG_MODE: True=命令行交互模式, False=后端API服务模式
DEBUG_MODE = False

# CSV文件路径
CSV_FILE = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

# 图表保存目录
CHART_DIR = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/charts"
os.makedirs(CHART_DIR, exist_ok=True)

# ==================== 工具定义 ====================

def _parse_time_range(time_filter: str = None) -> tuple:
    """
    解析自然语言时间范围表达式

    Args:
        time_filter: 时间过滤表达式，如：
            - "过去一小时" / "最近1小时" / "1h"
            - "过去两小时" / "2h"
            - "今天上午" / "上午"
            - "今天下午" / "下午"
            - "今天" / "今日"
            - "昨天"
            - "过去N分钟" / "Nmin" (如 "30分钟" / "30min")
            - "下午五点到六点" / "17点到18点" / "5点到6点" (具体时间段)

    Returns:
        (start_time, end_time) 或 (None, None)
    """
    if not time_filter:
        return None, None

    import re
    now = datetime.now()
    time_filter_original = time_filter.strip()
    time_filter = time_filter_original.lower()

    # ========== 具体时间段解析 (优先处理) ==========
    # 匹配模式: "X点到Y点" / "X点至Y点" / "X点-Y点" 等
    # 支持: "下午五点到六点", "17点到18点", "5点到6点", "5:00到6:00"

    # 中文数字映射
    cn_hour_map = {
        '零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4,
        '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
        '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '二十一': 21, '二十二': 22, '二十三': 23
    }

    def parse_hour(s: str, is_afternoon: bool = False, is_morning: bool = False) -> int:
        """解析小时，支持中文和阿拉伯数字"""
        s = s.strip()
        hour = None
        # 阿拉伯数字
        if s.isdigit():
            hour = int(s)
        # 中文数字
        else:
            for cn, num in cn_hour_map.items():
                if cn in s:
                    hour = num
                    break

        if hour is None:
            return None

        # 处理上午/下午
        if is_afternoon and hour < 12:
            hour += 12
        elif is_morning and hour == 12:
            hour = 0  # 凌晨12点 = 0点

        return hour

    # 检测时间段模式 (带"到"、"至"、"-"等连接词)
    time_range_patterns = [
        r'(\d+|[零一二两三四五六七八九十百]+)\s*点.*?(\d+|[零一二两三四五六七八九十百]+)\s*点',
        r'(\d{1,2}):(\d{2})\s*到\s*(\d{1,2}):(\d{2})',
        r'(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})',
    ]

    for pattern in time_range_patterns:
        match = re.search(pattern, time_filter)
        if match:
            try:
                # 判断上午/下午上下文
                is_afternoon = '下午' in time_filter
                is_morning = '上午' in time_filter or '凌晨' in time_filter or '早上' in time_filter

                if ':' in match.group(0):  # HH:MM格式
                    h1, m1, h2, m2 = match.groups()
                    start_hour, start_min = int(h1), int(m1)
                    end_hour, end_min = int(h2), int(m2)
                    # 处理12小时制到24小时制
                    if is_afternoon and start_hour < 12:
                        start_hour += 12
                    if is_afternoon and end_hour < 12:
                        end_hour += 12
                else:  # X点格式
                    h1_str, h2_str = match.groups()
                    start_hour = parse_hour(h1_str, is_afternoon=is_afternoon, is_morning=is_morning)
                    end_hour = parse_hour(h2_str, is_afternoon=is_afternoon, is_morning=is_morning)
                    start_min = end_min = 0
                    if start_hour is None or end_hour is None:
                        continue

                # 判断日期 (今天/昨天/默认今天)
                day_offset = 0
                if '昨天' in time_filter:
                    day_offset = -1
                elif '明天' in time_filter:
                    day_offset = 1

                base_date = (now + pd.Timedelta(days=day_offset)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

                start_time = base_date + pd.Timedelta(hours=start_hour, minutes=start_min)
                end_time = base_date + pd.Timedelta(hours=end_hour, minutes=end_min)

                return start_time, end_time
            except (ValueError, IndexError):
                continue

    # ========== 原有的相对时间解析 ==========

    # 按小时
    if any(kw in time_filter for kw in ['过去一小时', '最近1小时', '1小时', '1h']):
        return now - pd.Timedelta(hours=1), now
    elif '2h' in time_filter or '2小时' in time_filter:
        return now - pd.Timedelta(hours=2), now
    elif '3h' in time_filter or '3小时' in time_filter:
        return now - pd.Timedelta(hours=3), now
    elif '6h' in time_filter or '6小时' in time_filter:
        return now - pd.Timedelta(hours=6), now
    elif '12h' in time_filter or '12小时' in time_filter:
        return now - pd.Timedelta(hours=12), now
    elif '24h' in time_filter or '24小时' in time_filter:
        return now - pd.Timedelta(hours=24), now
    # 中文数字小时
    elif '两小时' in time_filter or '二小时' in time_filter:
        return now - pd.Timedelta(hours=2), now
    elif '三小时' in time_filter:
        return now - pd.Timedelta(hours=3), now
    elif '六小时' in time_filter:
        return now - pd.Timedelta(hours=6), now
    elif '十二小时' in time_filter:
        return now - pd.Timedelta(hours=12), now

    # 按分钟
    if 'min' in time_filter or '分钟' in time_filter:
        # 阿拉伯数字: "5分钟" / "5min"
        match = re.search(r'(\d+)\s*(min|分钟)', time_filter)
        if match:
            mins = int(match.group(1))
            return now - pd.Timedelta(minutes=mins), now
        # 中文数字: 长词优先匹配，避免"三十"被匹配成"三"
        chinese_nums = [
            ('六十', 60), ('五十', 50), ('四十', 40), ('三十', 30),
            ('二十', 20), ('十五', 15), ('十二', 12), ('十一', 11),
            ('十', 10), ('九', 9), ('八', 8), ('七', 7), ('六', 6),
            ('五', 5), ('四', 4), ('三', 3), ('二', 2), ('两', 2), ('一', 1)
        ]
        for cn_num, val in chinese_nums:
            if cn_num in time_filter and '分钟' in time_filter:
                return now - pd.Timedelta(minutes=val), now

    # 今天上午 (00:00 - 12:00)
    if any(kw in time_filter for kw in ['今天上午', '上午', '今上午']):
        morning_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        morning_end = now.replace(hour=12, minute=0, second=0, microsecond=0)
        return morning_start, morning_end

    # 今天下午 (12:00 - 23:59:59)
    if any(kw in time_filter for kw in ['今天下午', '下午', '今下午']):
        afternoon_start = now.replace(hour=12, minute=0, second=0, microsecond=0)
        afternoon_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        return afternoon_start, afternoon_end

    # 今天
    if any(kw in time_filter for kw in ['今天', '今日']):
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return day_start, now

    # 昨天
    if '昨天' in time_filter:
        yesterday = now - pd.Timedelta(days=1)
        day_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return day_start, day_end

    return None, None


def _apply_time_filter(df: pd.DataFrame, time_filter: str = None) -> pd.DataFrame:
    """
    应用时间过滤到DataFrame

    Args:
        df: 包含datetime列的DataFrame
        time_filter: 时间过滤表达式

    Returns:
        过滤后的DataFrame
    """
    if time_filter:
        start_time, end_time = _parse_time_range(time_filter)
        if start_time and end_time:
            df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)].copy()
    return df


def generate_price_chart(limit: int = 100, time_filter: str = None) -> str:
    """
    读取CSV数据并生成价格走势图，上传到DDF文件系统

    Args:
        limit: 最多读取N条记录，默认100条（当指定time_filter时作为上限）
        time_filter: 时间过滤表达式，如：
            - "过去一小时" / "1h"
            - "今天上午" / "上午"
            - "今天下午" / "下午"
            - "今天" / "今日"
            - "昨天"
            - "30分钟" / "30min"

    Returns:
        JSON格式结果，包含图表预览链接
    """
    try:
        # 检查文件是否存在
        if not os.path.exists(CSV_FILE):
            result = {
                "status": "ERROR",
                "message": f"数据文件不存在: {CSV_FILE}"
            }
            return json.dumps(result, ensure_ascii=False)

        # 读取CSV数据
        df = pd.read_csv(CSV_FILE)

        if df.empty:
            result = {
                "status": "ERROR",
                "message": "CSV文件为空"
            }
            return json.dumps(result, ensure_ascii=False)

        # 转换时间戳为datetime
        df['datetime'] = pd.to_datetime(df['datetime'])

        # 先应用时间过滤
        if time_filter:
            df = _apply_time_filter(df, time_filter)
            if df.empty:
                result = {
                    "status": "ERROR",
                    "message": f"时间范围 '{time_filter}' 内没有数据"
                }
                return json.dumps(result, ensure_ascii=False)

        # 按时间排序
        df = df.sort_values('timestamp')

        # 限制读取的记录数
        # 当指定时间过滤时，不限制记录数（显示完整时间范围的数据）
        # 当未指定时间过滤时，使用limit作为记录数上限
        if not time_filter and len(df) > limit:
            df = df.tail(limit).copy()

        # 创建图表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        # 价格走势图
        ax1.plot(df['datetime'], df['price'], label='价格', color='#2E86AB', linewidth=1.5)
        ax1.set_ylabel('价格 (USDT)', fontsize=12)
        # 根据是否有时间过滤显示不同标题
        if time_filter:
            ax1.set_title(f'BTC/USDT 价格走势 ({time_filter}, 共{len(df)}条记录)', fontsize=14)
        else:
            ax1.set_title(f'BTC/USDT 价格走势 (最近{len(df)}条记录)', fontsize=14)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)

        # 成交量图
        ax2.bar(df['datetime'], df['volume'], label='成交量', color='#A23B72', alpha=0.6)
        ax2.set_ylabel('成交量 (BTC)', fontsize=12)
        ax2.set_xlabel('时间', fontsize=12)
        ax2.set_title('成交量分布', fontsize=14)
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)

        plt.tight_layout()

        # 保存图表
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"btc_price_chart_{timestamp}.png"
        chart_path = os.path.join(CHART_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        # 上传到DDF文件系统
        from drsai.utils.utils import upload_to_hepai_filesystem

        file_obj = upload_to_hepai_filesystem(chart_path)
        preview_url = file_obj.get("url", "")

        # 统计信息
        min_price = df['price'].min()
        max_price = df['price'].max()
        avg_price = df['price'].mean()
        price_range = max_price - min_price

        result = {
            "status": "SUCCESS",
            "message": "图表生成并上传成功",
            "preview_url": preview_url,
            "chart_filename": chart_filename,
            "record_count": len(df),
            "price_stats": {
                "min": round(min_price, 2),
                "max": round(max_price, 2),
                "avg": round(avg_price, 2),
                "range": round(price_range, 2)
            },
            "time_range": {
                "start": df['datetime'].min().strftime("%Y-%m-%d %H:%M:%S"),
                "end": df['datetime'].max().strftime("%Y-%m-%d %H:%M:%S")
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"生成图表失败: {str(e)}"
        }
        return json.dumps(result, ensure_ascii=False)


def generate_volume_distribution_chart(limit: int = 100, time_filter: str = None) -> str:
    """
    生成成交量分布图

    Args:
        limit: 最多读取N条记录（当指定time_filter时作为上限）
        time_filter: 时间过滤表达式，如：
            - "过去一小时" / "1h"
            - "今天上午" / "上午"
            - "今天下午" / "下午"
            - "今天" / "今日"
            - "昨天"
            - "30分钟" / "30min"

    Returns:
        JSON格式结果
    """
    try:
        if not os.path.exists(CSV_FILE):
            result = {
                "status": "ERROR",
                "message": f"数据文件不存在: {CSV_FILE}"
            }
            return json.dumps(result, ensure_ascii=False)

        df = pd.read_csv(CSV_FILE)

        if df.empty:
            result = {
                "status": "ERROR",
                "message": "CSV文件为空"
            }
            return json.dumps(result, ensure_ascii=False)

        df['datetime'] = pd.to_datetime(df['datetime'])

        # 先应用时间过滤
        if time_filter:
            df = _apply_time_filter(df, time_filter)
            if df.empty:
                result = {
                    "status": "ERROR",
                    "message": f"时间范围 '{time_filter}' 内没有数据"
                }
                return json.dumps(result, ensure_ascii=False)

        # 限制读取的记录数
        # 当指定时间过滤时，不限制记录数（显示完整时间范围的数据）
        # 当未指定时间过滤时，使用limit作为记录数上限
        if not time_filter and len(df) > limit:
            df = df.tail(limit).copy()

        # 创建成交量分布直方图
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

        # 保存并上传
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"volume_distribution_{timestamp}.png"
        chart_path = os.path.join(CHART_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        from drsai.utils.utils import upload_to_hepai_filesystem
        file_obj = upload_to_hepai_filesystem(chart_path)
        preview_url = file_obj.get("url", "")

        result = {
            "status": "SUCCESS",
            "message": "成交量分布图生成并上传成功",
            "preview_url": preview_url,
            "chart_filename": chart_filename,
            "record_count": len(df),
            "time_filter": time_filter or "",
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"生成图表失败: {str(e)}"
        }
        return json.dumps(result, ensure_ascii=False)


def get_csv_summary(limit: int = 10, time_filter: str = None) -> str:
    """
    获取CSV数据摘要统计

    Args:
        limit: 显示最近N条数据（当指定time_filter时作为上限）
        time_filter: 时间过滤表达式，如：
            - "过去一小时" / "1h"
            - "今天上午" / "上午"
            - "今天下午" / "下午"
            - "今天" / "今日"
            - "昨天"
            - "30分钟" / "30min"

    Returns:
        JSON格式统计信息
    """
    try:
        if not os.path.exists(CSV_FILE):
            result = {
                "status": "ERROR",
                "message": f"数据文件不存在: {CSV_FILE}"
            }
            return json.dumps(result, ensure_ascii=False)

        df = pd.read_csv(CSV_FILE)

        if df.empty:
            result = {
                "status": "ERROR",
                "message": "CSV文件为空"
            }
            return json.dumps(result, ensure_ascii=False)

        df['datetime'] = pd.to_datetime(df['datetime'])

        # 应用时间过滤
        if time_filter:
            df = _apply_time_filter(df, time_filter)
            if df.empty:
                result = {
                    "status": "ERROR",
                    "message": f"时间范围 '{time_filter}' 内没有数据"
                }
                return json.dumps(result, ensure_ascii=False)

        total_records = len(df)
        df_sample = df.tail(limit).copy()

        stats = {
            "total_records": total_records,
            "sample_records": limit if total_records > limit else total_records,
            "time_filter": time_filter or "",
            "price": {
                "min": float(df['price'].min()),
                "max": float(df['price'].max()),
                "avg": float(df['price'].mean()),
                "std": float(df['price'].std())
            },
            "volume": {
                "min": float(df['volume'].min()),
                "max": float(df['volume'].max()),
                "avg": float(df['volume'].mean()),
                "total": float(df['volume'].sum())
            },
            "latest_data": df_sample.tail(3).to_dict('records'),
            "time_range": {
                "start": df['datetime'].min().strftime("%Y-%m-%d %H:%M:%S"),
                "end": df['datetime'].max().strftime("%Y-%m-%d %H:%M:%S")
            },
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        if time_filter:
            message = f"数据统计完成 ({time_filter})，共{total_records}条记录"
        else:
            message = f"数据统计完成，共{total_records}条记录"

        result = {
            "status": "SUCCESS",
            "message": message,
            "data": stats
        }
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"统计失败: {str(e)}"
        }
        return json.dumps(result, ensure_ascii=False)


# ==================== 智能体系统 ====================
from drsai import AssistantAgent, HepAIChatCompletionClient, Console, run_worker, run_console
from drsai.modules.managers.database import DatabaseManager


def create_chart_agent(
    api_key: str = None,
    thread_id: str = None,
    user_id: str = None,
    db_manager: DatabaseManager = None
):
    """
    创建数据可视化Agent（工厂函数）
    """
    # 创建模型客户端
    model_client = HepAIChatCompletionClient(
        model="deepseek-ai/deepseek-v3",
        api_key=api_key or os.environ.get("HEPAI_API_KEY"),
        base_url="https://aiapi.ihep.ac.cn/apiv2"
    )

    return AssistantAgent(
        name="chart_agent",
        model_client=model_client,
        tools=[generate_price_chart, generate_volume_distribution_chart, get_csv_summary],
        system_message="""你是一个数据可视化助手。你的任务是读取CSV数据文件并生成图表。

可用的功能：
1. 生成价格走势图 - 调用 generate_price_chart 工具
2. 生成成交量分布图 - 调用 generate_volume_distribution_chart 工具
3. 获取数据统计 - 调用 get_csv_summary 工具

工具参数说明：
- limit: 最多读取N条记录（默认100），作为数据量上限
- time_filter: 时间过滤表达式，支持自然语言，如：
  * 按小时: "过去一小时" / "1h" / "2h" / "6h" / "12h" / "24h"
  * 按分钟: "30分钟" / "30min" / "60分钟" / "60min"
  * 按时段: "今天上午" / "上午" / "今天下午" / "下午"
  * 按天: "今天" / "今日" / "昨天"

工作流程：
- 根据用户需求选择合适的过滤方式：
  * 如果用户指定时间范围（如"今天上午的数据"），使用time_filter参数
  * 如果用户指定记录数（如"最近100条"），使用limit参数
  * 如果用户两者都指定，先按时间过滤，再限制记录数上限
- 调用相应工具生成图表并上传
- 向用户展示图表链接！使用Markdown格式: ![图表](preview_url)
- 同时显示图表的统计信息（记录数、价格区间、时间范围等）
- 报告必须包含系统时间

重要：
- 必须向用户展示图表预览链接
- 用简洁清晰的语言向用户报告
- 不要在输出中显示"调用工具"等技术细节""",
        reflect_on_tool_use=False,
        tool_call_summary_format="操作结果：\n{result}\n",
        tool_call_summary_prompt="""请根据上方操作结果的JSON数据回复用户：

如果 status 是 "SUCCESS"：
- 告诉用户"✓ 图表生成成功"
- 使用Markdown格式展示图表: ![价格走势图](preview_url)
- 显示统计信息：记录数、价格区间（最高价、最低价、平均价）、时间范围
- 显示系统时间
- 询问是否需要生成其他图表

如果 status 是 "ERROR"：
- 告诉用户"✗ 图表生成失败"
- 显示错误信息

对于 get_csv_summary 的结果：
- 显示总记录数、价格统计（最高、最低、平均）
- 显示最近几条数据
- 显示系统时间

重要：用简洁的中文回复，不要提及"工具"、"JSON"、"调用"等技术术语。
必须展示图表预览链接，必须包含系统时间。""",
        thread_id=thread_id,
        db_manager=db_manager,
        user_id=user_id,
    )


async def run_console_mode():
    """运行Agent（命令行模式）"""
    print("="*60)
    print("=== 数据可视化Agent（命令行模式）===")
    print("="*60 + "\n")

    await run_console(agent_factory=create_chart_agent, task="请介绍你的功能")


async def run_backend_service():
    """启动后端API服务"""
    await run_worker(
        # 智能体注册信息
        agent_name="chart_agent",
        author="lin@ihep.ac.cn",
        permission='groups: drsai; users: admin, lin@ihep.ac.cn; owner: lin@ihep.ac.cn',
        description="数据可视化Agent：读取CSV数据生成价格走势图和成交量分布图，上传到DDF文件系统并返回预览链接。",
        version="1.0.0",
        logo="https://aiapi.ihep.ac.cn/apiv2/files/file-8572b27d093f4e15913bebfac3645e20/preview",
        # 前端展示的示例
        examples=[
            "生成价格走势图",
            "生成成交量分布图",
            "查看数据统计",
            "最近100条数据的价格趋势",
            "今天上午的价格走势",
            "过去一小时的数据统计",
            "昨天的成交量分布",
        ],
        # 智能体工厂
        agent_factory=create_chart_agent,
        # 后端服务配置
        host="0.0.0.0",
        port=43814,
        no_register=False,
        enable_openwebui_pipeline=True,
        history_mode="backend",
        use_api_key_mode="backend",
    )


# ==================== 测试函数 ====================

def test_tools():
    """测试工具函数"""
    print("="*60)
    print("=== 数据可视化Agent工具测试 ===")
    print("="*60)

    # 测试生成价格走势图
    print("\n1. 生成价格走势图:")
    result = json.loads(generate_price_chart(limit=100))
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    if result['status'] == 'SUCCESS':
        print(f"Preview URL: {result['preview_url']}")
        print(f"Price Range: {result['price_stats']['min']} - {result['price_stats']['max']}")

    print("\n2. 获取数据统计:")
    stats = json.loads(get_csv_summary(limit=5))
    print(f"Status: {stats['status']}")
    if stats['status'] == 'SUCCESS':
        data = stats['data']
        print(f"Total Records: {data['total_records']}")
        print(f"Avg Price: {data['price']['avg']}")

    print("\n" + "="*60)
    print("=== 测试完成 ===")
    print("="*60)


if __name__ == "__main__":
    if DEBUG_MODE:
        # 测试工具函数
        test_tools()
    else:
        # 后端API服务模式
        asyncio.run(run_backend_service())
