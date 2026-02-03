# DrSai 数据获取管理Agent
# 控制后台运行 data_get.py 获取实时数据

import asyncio
import os
import subprocess
import signal
import json
import csv
from datetime import datetime, timedelta
from pathlib import Path

# ==================== 配置 ====================
# DEBUG_MODE: True=命令行交互模式, False=后端API服务模式
DEBUG_MODE = True

# data_get.py 脚本路径
DATA_GET_SCRIPT = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python/data_get.py"
PYTHON_EXECUTABLE = "/datafs/users/lin/python-venv/drsai/bin/python"

# 存储后台进程信息
background_processes = {}


# ==================== 工具定义 ====================

def check_data_quality(check_count: int = 10) -> str:
    """
    检查CSV文件中最新数据的quality（price是否为0）

    Args:
        check_count: 检查最新N条数据，默认10条

    Returns:
        JSON格式的检查结果
    """
    csv_path = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

    if not os.path.exists(csv_path):
        result = {
            "status": "NO_FILE",
            "message": "数据文件不存在",
            "file_path": csv_path
        }
        return json.dumps(result, ensure_ascii=False)

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if len(rows) == 0:
            result = {
                "status": "NO_DATA",
                "message": "数据文件为空",
                "file_path": csv_path
            }
            return json.dumps(result, ensure_ascii=False)

        # 获取最新N条数据
        latest_rows = rows[-check_count:] if len(rows) >= check_count else rows

        # 检查price是否为0
        zero_count = 0
        valid_count = 0
        prices = []

        for row in latest_rows:
            price = float(row.get('price', 0))
            prices.append(price)
            if price == 0:
                zero_count += 1
            else:
                valid_count += 1

        # 获取最新数据的时间
        latest_time = latest_rows[-1].get('datetime', 'Unknown')
        latest_price = float(latest_rows[-1].get('price', 0))

        # 判断数据质量
        if zero_count == len(latest_rows):
            # 全部是0
            result = {
                "status": "ALL_ZERO",
                "message": "⚠ 数据写入异常：最新数据全是占位数据（price=0）",
                "checked_count": len(latest_rows),
                "zero_count": zero_count,
                "valid_count": valid_count,
                "latest_price": latest_price,
                "latest_time": latest_time,
                "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "suggestion": "建议重启数据获取服务"
            }
        elif zero_count > len(latest_rows) // 2:
            # 超过一半是0
            result = {
                "status": "MANY_ZERO",
                "message": f"⚠ 数据质量异常：最新{len(latest_rows)}条数据中有{zero_count}条是占位数据",
                "checked_count": len(latest_rows),
                "zero_count": zero_count,
                "valid_count": valid_count,
                "latest_price": latest_price,
                "latest_time": latest_time,
                "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "suggestion": "建议检查网络连接或重启服务"
            }
        else:
            # 正常
            result = {
                "status": "OK",
                "message": "✓ 数据获取运行正常",
                "checked_count": len(latest_rows),
                "zero_count": zero_count,
                "valid_count": valid_count,
                "latest_price": latest_price,
                "latest_time": latest_time,
                "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"检查数据质量失败: {str(e)}",
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)


def start_data_collection(duration: int = 0) -> str:
    """
    启动后台数据获取服务（使用nohup方式）

    Args:
        duration: 预计运行时长（秒），0表示持续运行

    Returns:
        JSON格式的启动结果
    """
    # 检查脚本是否存在
    if not os.path.exists(DATA_GET_SCRIPT):
        result = {
            "status": "ERROR",
            "message": f"数据获取脚本不存在: {DATA_GET_SCRIPT}"
        }
        return json.dumps(result, ensure_ascii=False)

    # 先检查系统中是否已有data_get.py在运行
    try:
        existing_result = subprocess.run(
            ["pgrep", "-f", "data_get.py"],
            capture_output=True,
            text=True
        )
        if existing_result.returncode == 0:
            # 已有进程在运行，检查数据质量
            quality_check = json.loads(check_data_quality())
            pids = existing_result.stdout.strip().split('\n')

            result = {
                "status": "ALREADY_RUNNING",
                "message": "数据获取服务已在运行中",
                "pids": pids,
                "data_quality": quality_check.get("status", "UNKNOWN"),
                "quality_message": quality_check.get("message", ""),
                "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            return json.dumps(result, ensure_ascii=False)
    except:
        pass

    # 检查内存中是否已记录
    if "data_get" in background_processes and background_processes["data_get"]["process"].poll() is None:
        quality_check = json.loads(check_data_quality())
        result = {
            "status": "ALREADY_RUNNING",
            "message": "数据获取服务已在运行中",
            "pid": background_processes["data_get"]["pid"],
            "start_time": background_processes["data_get"]["start_time"],
            "data_quality": quality_check.get("status", "UNKNOWN"),
            "quality_message": quality_check.get("message", "")
        }
        return json.dumps(result, ensure_ascii=False)

    try:
        # 使用nohup方式启动后台进程
        output_file = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python/nohup.out"

        # 构建nohup命令
        cmd = f"nohup {PYTHON_EXECUTABLE} {DATA_GET_SCRIPT} > {output_file} 2>&1 &"
        subprocess.run(cmd, shell=True, check=True)

        # 等待一下让进程启动
        import time
        time.sleep(2)

        # 获取新启动的进程PID
        try:
            pid_result = subprocess.run(
                ["pgrep", "-f", "data_get.py"],
                capture_output=True,
                text=True
            )
            if pid_result.returncode == 0:
                pids = pid_result.stdout.strip().split('\n')
                latest_pid = pids[-1] if pids else "Unknown"
            else:
                latest_pid = "Unknown"
        except:
            latest_pid = "Unknown"

        result = {
            "status": "STARTED",
            "message": "✓ 数据获取服务已启动（使用nohup方式）",
            "pid": latest_pid,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output_file": output_file,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"启动失败: {str(e)}",
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)


def restart_data_collection() -> str:
    """
    重启数据获取服务（先停止再启动）

    Returns:
        JSON格式的重启结果
    """
    # 先停止
    stop_result = json.loads(stop_data_collection())

    # 等待一下
    import time
    time.sleep(1)

    # 再启动
    start_result = json.loads(start_data_collection())

    result = {
        "status": "RESTARTED",
        "message": "数据获取服务已重启",
        "stop_result": stop_result.get("message", ""),
        "start_result": start_result.get("message", ""),
        "new_pid": start_result.get("pid", "Unknown"),
        "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return json.dumps(result, ensure_ascii=False)


def stop_data_collection() -> str:
    """
    停止后台数据获取服务（增强版：可以处理孤儿进程）

    Returns:
        JSON格式的停止结果
    """
    stopped_pids = []
    orphan_pids = []

    # 首先尝试停止内存中记录的进程
    if "data_get" in background_processes:
        process_info = background_processes["data_get"]
        process = process_info["process"]

        # 检查进程是否还在运行（通过系统检查）
        try:
            os.kill(process_info["pid"], 0)
            # 进程存在，尝试停止
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=5)
                stopped_pids.append(str(process_info["pid"]))
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                process.wait()
                stopped_pids.append(str(process_info["pid"]))
            except:
                # 进程可能已经死了
                pass
        except OSError:
            # 进程不存在
            pass

        del background_processes["data_get"]

    # 其次，检查并清理系统中可能存在的孤儿进程
    try:
        result = subprocess.run(
            ["pgrep", "-f", "data_get.py"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            for pid_str in pids:
                if pid_str and pid_str not in stopped_pids:
                    try:
                        os.kill(int(pid_str), signal.SIGTERM)
                        orphan_pids.append(pid_str)
                        time.sleep(0.1)  # 等待进程退出
                        # 检查是否已退出，如果没有则强制kill
                        try:
                            os.kill(int(pid_str), 0)
                            os.kill(int(pid_str), signal.SIGKILL)
                        except OSError:
                            pass  # 进程已退出
                    except:
                        pass
    except:
        pass

    # 构建返回结果
    if stopped_pids or orphan_pids:
        result = {
            "status": "STOPPED",
            "message": "数据获取服务已停止",
            "stopped_pids": stopped_pids if stopped_pids else ["无"],
            "orphan_pids_cleaned": orphan_pids if orphan_pids else ["无"],
            "stop_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)
    else:
        result = {
            "status": "NOT_RUNNING",
            "message": "数据获取服务未在运行",
            "stop_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)


def get_data_collection_status(check_count: int = 10) -> str:
    """
    获取数据获取服务状态（增强版：检查系统中运行的进程和CSV数据质量）

    Args:
        check_count: 检查最新N条数据的质量，默认10条

    Returns:
        JSON格式的状态信息
    """
    csv_path = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

    # 首先检查系统中是否有data_get.py进程在运行
    running_pids = []
    try:
        result = subprocess.run(
            ["pgrep", "-f", "data_get.py"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            running_pids = result.stdout.strip().split('\n')
    except:
        pass

    # 如果没有进程在运行
    if not running_pids:
        result = {
            "status": "NOT_RUNNING",
            "message": "数据获取服务未在运行",
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "suggestion": "是否需要启动数据获取服务？"
        }
        return json.dumps(result, ensure_ascii=False)

    # 有进程在运行，检查数据质量
    quality_check = json.loads(check_data_quality(check_count))
    quality_status = quality_check.get("status", "UNKNOWN")

    # 获取CSV文件基本信息
    csv_last_write_time = None
    csv_record_count = 0

    if os.path.exists(csv_path):
        mtime = os.path.getmtime(csv_path)
        csv_last_write = datetime.fromtimestamp(mtime)
        csv_last_write_time = csv_last_write.strftime("%Y-%m-%d %H:%M:%S")

        try:
            with open(csv_path, 'r') as f:
                csv_record_count = sum(1 for _ in f) - 1  # 减去表头
        except:
            csv_record_count = 0

    # 根据数据质量返回不同的状态
    if quality_status == "ALL_ZERO":
        result = {
            "status": "DATA_ERROR",
            "message": "⚠ 数据写入异常：最新数据全是占位数据（price=0），可能网络断开",
            "running_pids": running_pids,
            "data_quality": quality_status,
            "checked_count": quality_check.get("checked_count", check_count),
            "zero_count": quality_check.get("zero_count", 0),
            "latest_price": quality_check.get("latest_price", 0),
            "latest_time": quality_check.get("latest_time", ""),
            "csv_last_write": csv_last_write_time,
            "csv_record_count": csv_record_count,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "suggestion": "建议重启数据获取服务"
        }
    elif quality_status == "MANY_ZERO":
        result = {
            "status": "DATA_WARNING",
            "message": f"⚠ 数据质量异常：最新{check_count}条数据中有{quality_check.get('zero_count', 0)}条是占位数据",
            "running_pids": running_pids,
            "data_quality": quality_status,
            "checked_count": quality_check.get("checked_count", check_count),
            "zero_count": quality_check.get("zero_count", 0),
            "valid_count": quality_check.get("valid_count", 0),
            "latest_price": quality_check.get("latest_price", 0),
            "latest_time": quality_check.get("latest_time", ""),
            "csv_last_write": csv_last_write_time,
            "csv_record_count": csv_record_count,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "suggestion": "建议检查网络连接"
        }
    elif quality_status == "OK":
        result = {
            "status": "RUNNING_OK",
            "message": f"✓ 数据获取服务运行正常（最新{check_count}条数据全部有效）",
            "running_pids": running_pids,
            "data_quality": quality_status,
            "checked_count": quality_check.get("checked_count", check_count),
            "zero_count": quality_check.get("zero_count", 0),
            "valid_count": quality_check.get("valid_count", check_count),
            "latest_price": quality_check.get("latest_price", 0),
            "latest_time": quality_check.get("latest_time", ""),
            "csv_last_write": csv_last_write_time,
            "csv_record_count": csv_record_count,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    else:
        # 其他状态（NO_FILE, NO_DATA, ERROR等）
        result = {
            "status": quality_status,
            "message": quality_check.get("message", "数据获取服务运行中"),
            "running_pids": running_pids,
            "csv_last_write": csv_last_write_time,
            "csv_record_count": csv_record_count,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    return json.dumps(result, ensure_ascii=False)


def get_csv_file_info() -> str:
    """
    获取CSV数据文件信息

    Returns:
        JSON格式的文件信息
    """
    csv_path = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"

    if not os.path.exists(csv_path):
        result = {
            "status": "NO_FILE",
            "message": "数据文件不存在",
            "file_path": csv_path
        }
        return json.dumps(result, ensure_ascii=False)

    try:
        # 统计文件行数（包括表头）
        with open(csv_path, 'r') as f:
            line_count = sum(1 for _ in f)

        # 获取文件大小
        file_size = os.path.getsize(csv_path)

        # 获取文件修改时间
        mtime = os.path.getmtime(csv_path)
        mod_time = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "status": "EXISTS",
            "message": "数据文件存在",
            "file_path": csv_path,
            "total_records": line_count - 1,  # 减去表头
            "file_size_bytes": file_size,
            "last_modified": mod_time,
            "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        result = {
            "status": "ERROR",
            "message": f"读取文件信息失败: {str(e)}",
            "file_path": csv_path
        }
        return json.dumps(result, ensure_ascii=False)


# ==================== 智能体系统 ====================
from drsai import AssistantAgent, HepAIChatCompletionClient, Console, run_worker, run_console
from drsai.modules.managers.database import DatabaseManager


def create_data_get_agent(
    api_key: str = None,
    thread_id: str = None,
    user_id: str = None,
    db_manager: DatabaseManager = None
):
    """
    创建数据获取管理Agent（工厂函数）

    Args:
        api_key: API密钥
        thread_id: 会话ID
        user_id: 用户ID
        db_manager: 数据库管理器
    """
    # 创建模型客户端
    model_client = HepAIChatCompletionClient(
        model="deepseek-ai/deepseek-v3",
        api_key=api_key or os.environ.get("HEPAI_API_KEY"),
        base_url="https://aiapi.ihep.ac.cn/apiv2"
    )

    return AssistantAgent(
        name="data_get_agent",
        model_client=model_client,
        tools=[check_data_quality, start_data_collection, stop_data_collection, get_data_collection_status, get_csv_file_info, restart_data_collection],
        system_message="""你是一个数据获取管理助手。你的任务是管理后台数据获取服务（data_get.py）。

可用的功能：
1. 检查数据质量 - 调用 check_data_quality 工具
2. 启动数据获取服务 - 调用 start_data_collection 工具（使用nohup方式）
3. 停止数据获取服务 - 调用 stop_data_collection 工具
4. 重启数据获取服务 - 调用 restart_data_collection 工具
5. 查询服务状态 - 调用 get_data_collection_status 工具（会检查最新数据是否为0）
6. 查看数据文件信息 - 调用 get_csv_file_info 工具

工作流程：
- 用户询问系统状态时，调用 get_data_collection_status，会自动检查CSV最新10条数据的price是否为0
- 如果最新数据的price为0，告知用户"数据写入异常"
- 如果最新数据的price不为0，告知用户"数据获取运行正常"
- 用户要求启动服务时，先检查是否已运行，然后使用nohup方式启动
- 用户要求重启服务时，调用 restart_data_collection

重要：
- 报告状态时必须包含系统时间
- 用简洁清晰的语言向用户报告状态
- 不要在输出中显示"调用工具"等技术细节""",
        reflect_on_tool_use=False,
        tool_call_summary_format="操作结果：\n{result}\n",
        tool_call_summary_prompt="""请根据上方操作结果的JSON数据回复用户：

如果 status 是 "RUNNING_OK"：
- 告诉用户"✓ 数据获取服务运行正常"
- 显示进程PID、最新价格、最新时间、记录总数、系统时间

如果 status 是 "DATA_ERROR" 或数据全是0：
- 警告用户"⚠ 数据写入异常：最新数据全是占位数据（price=0），可能网络断开"
- 显示进程PID、建议重启服务

如果 status 是 "DATA_WARNING"：
- 警告用户"⚠ 数据质量异常：部分数据是占位数据"
- 显示有效数据数量和占位数据数量

如果 status 是 "NOT_RUNNING"：
- 告诉用户"数据获取服务未在运行"
- 询问是否需要启动

如果 status 是 "STARTED"：
- 告诉用户"✓ 数据获取服务已启动（使用nohup方式）"
- 显示PID和启动时间

如果 status 是 "ALREADY_RUNNING"：
- 告诉用户"数据获取服务已在运行中"
- 显示数据质量状态

如果 status 是 "RESTARTED"：
- 告诉用户"✓ 数据获取服务已重启"
- 显示新的PID

如果 status 是 "STOPPED"：
- 告诉用户"✓ 数据获取服务已停止"

对于 check_data_quality 的结果：
- status 是 "OK"：告诉用户"数据质量正常"
- status 是 "ALL_ZERO"：警告用户"数据全是占位数据"

重要：用简洁的中文回复，不要提及"工具"、"JSON"、"调用"等技术术语。
所有报告必须包含系统时间。""",
        thread_id=thread_id,
        db_manager=db_manager,
        user_id=user_id,
    )


async def run_console_mode():
    """运行Agent（命令行模式）"""
    print("="*60)
    print("=== 数据获取管理Agent（命令行模式）===")
    print("="*60 + "\n")

    await run_console(agent_factory=create_data_get_agent, task="重启数据获取服务")


async def run_backend_service():
    """启动后端API服务"""
    await run_worker(
        # 智能体注册信息
        agent_name="data_get_agent",
        author="lin@ihep.ac.cn",
        permission='groups: drsai; users: admin, lin@ihep.ac.cn; owner: lin@ihep.ac.cn',
        description="数据获取管理Agent：控制后台运行数据获取服务，支持启动、停止、状态查询和数据文件管理。",
        version="1.0.0",
        logo="https://aiapi.ihep.ac.cn/apiv2/files/file-8572b27d093f4e15913bebfac3645e20/preview",
        # 前端展示的示例
        examples=[
            "启动数据获取",
            "停止数据获取",
            "查看服务状态",
            "查看数据文件信息",
        ],
        # 智能体工厂
        agent_factory=create_data_get_agent,
        # 后端服务配置
        host="0.0.0.0",
        port=42821,
        no_register=False,
        enable_openwebui_pipeline=True,
        history_mode="backend",
        use_api_key_mode="backend",
    )


if __name__ == "__main__":
    if DEBUG_MODE:
        # 命令行交互模式
        asyncio.run(run_console_mode())
    else:
        # 后端API服务模式
        asyncio.run(run_backend_service())
