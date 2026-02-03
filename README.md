# BTC 股票监控智能体系统

> **注意**：本项目是基于 [DrSai](https://github.com/hepai-lab/drsai) 框架学习后的初步尝试项目，用于实践和理解智能体开发。

## 项目简介

这是一个基于 DrSai 框架开发的 BTC 价格监控与分析智能体系统。系统集成了数据获取、数据分析、图表生成等功能，通过自然语言交互实现对 BTC 交易数据的实时监控和智能分析。

### 核心功能

- **实时数据获取**：通过 WebSocket 连接 Binance 获取 BTC 实时价格数据
- **数据获取管理**：启动/停止/重启数据获取服务，检查数据质量
- **数据分析**：价格趋势分析、波动性分析、异常检测、时间分布统计
- **图表生成**：价格走势图、成交量分布图，支持多种时间过滤条件
- **BTC 价格查询**：从本地 CSV 文件读取最新价格数据
- **自然语言交互**：支持中文自然语言查询

## 项目结构

```
stock_monitor/
├── python/                    # 核心代码目录
│   ├── stock_agent.py        # 主智能体（集成版，整合所有功能）
│   ├── data_get.py           # WebSocket 数据获取脚本
│   ├── data_get_agent.py     # 数据获取管理智能体（已整合到主智能体）
│   ├── data_analysis_agent.py # 数据分析智能体（已整合到主智能体）
│   ├── chart_agent.py        # 图表生成智能体（已整合到主智能体）
│   └── model_status.yaml     # 模型状态配置
├── data/                      # 数据存储目录
│   └── stock_data.csv        # BTC 交易数据（WebSocket 实时写入）
├── charts/                    # 图表生成目录
├── test_console.py           # 测试脚本
├── test.ipynb                # Jupyter 测试笔记
├── model_status.yaml         # 模型状态配置
└── README.md                 # 本文件
```

### 模块说明

#### 1. stock_agent.py（主智能体）

整合了所有功能的单一智能体系统，包含以下工具函数：

**数据获取管理**
- `check_data_quality` - 检查数据质量
- `get_data_collection_status` - 查询数据获取服务状态
- `start_data_collection` - 启动数据获取服务
- `stop_data_collection` - 停止数据获取服务
- `restart_data_collection` - 重启数据获取服务
- `get_csv_file_info` - 获取数据文件信息

**数据分析**
- `get_basic_stats` - 获取基本统计（价格、成交量统计）
- `analyze_trend` - 分析价格趋势（上涨/下跌/震荡）
- `analyze_volatility` - 分析价格波动性
- `analyze_time_distribution` - 按小时统计交易活跃度
- `compare_time_periods` - 比较两个时间段的数据
- `detect_price_anomalies` - 检测价格异常

**图表生成**
- `generate_price_chart` - 生成价格走势图（支持时间过滤）
- `generate_volume_distribution_chart` - 生成成交量分布图
- `get_csv_summary` - 获取数据摘要统计

**BTC 价格查询**
- `get_btc_realtime_data` - 获取 BTC 实时价格（从 CSV 读取）
- `get_market_status` - 获取市场状态

#### 2. data_get.py

WebSocket 数据获取脚本，连接 Binance 获取 BTC 实时价格数据并写入 CSV 文件。

**特性**：
- 自动重连机制
- 数据去重（基于 timestamp + symbol）
- 网络状态监控
- CSV 文件自动管理

#### 3. data_get_agent.py、data_analysis_agent.py、chart_agent.py

早期开发的独立智能体，功能已整合到 `stock_agent.py` 中。保留作为学习参考。

## 数据格式

CSV 文件格式（`data/stock_data.csv`）：

```csv
timestamp,datetime,symbol,price,volume
1737600000000,2025-01-23 10:00:00,BINANCE:BTCUSDT,98500.50,0.1234
...
```

## 技术栈

| 技术 | 说明 |
|------|------|
| **框架** | [DrSai](https://github.com/hepai-lab/drsai) - 基于 AutoGen 的科学智能体框架 |
| **模型** | DeepSeek V3 (via HepAI API) |
| **数据源** | Binance WebSocket API |
| **数据处理** | pandas, numpy |
| **可视化** | matplotlib |
| **存储** | CSV 文件 |

## 运行方式

### 1. 命令行测试模式

```bash
cd /data/juno/lin/agent/drsai-main/my_agent/stock_monitor
/datafs/users/lin/python-venv/drsai/bin/python python/stock_agent.py
```

### 2. 后端 Worker 服务模式

```bash
# 设置环境变量
export HEPAI_API_KEY=your_api_key

# 启动服务
/datafs/users/lin/python-venv/drsai/bin/python python/stock_agent.py
```

服务将注册为 HepAI Worker，可通过 DrSai-UI 或 OpenAI API 调用。

### 3. 单独启动数据获取服务

```bash
/datafs/users/lin/python-venv/drsai/bin/python python/data_get.py
```

## 使用示例

### 对话示例

```
用户: 查看 BTC 当前价格
智能体: [显示当前价格、成交量、更新时间等信息]

用户: 生成过去一小时的价格走势图
智能体: [生成图表并显示预览链接]

用户: 分析一下最近的价格趋势
智能体: [分析趋势并给出判断结果]

用户: 启动数据获取服务
智能体: [启动 data_get.py 后台进程]

用户: 检查数据获取服务状态
智能体: [显示进程状态、数据记录数、最新数据时间等]

用户: 检测价格异常
智能体: [使用统计方法检测异常价格并报告]
```

### 时间过滤支持

- 按分钟：`30分钟` / `30min`
- 按小时：`过去一小时` / `1h` / `2h` / `6h` / `12h` / `24h`
- 按时段：`今天上午` / `上午` / `今天下午` / `下午`
- 按天：`今天` / `今日` / `昨天`
- 具体时间段：`下午五点到六点` / `17点到18点`

## 配置说明

### 环境变量

```bash
# HepAI API 配置
export HEPAI_API_KEY=your_api_key
export HEPAI_BASE_URL=https://aiapi.ihep.ac.cn/apiv2
```

### 路径配置

代码中可配置的主要路径：

```python
CSV_FILE = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/data/stock_data.csv"
CHART_DIR = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/charts"
DATA_GET_SCRIPT = "/data/juno/lin/agent/drsai-main/my_agent/stock_monitor/python/data_get.py"
PYTHON_EXECUTABLE = "/datafs/users/lin/python-venv/drsai/bin/python"
```

## 学习笔记

本项目作为 DrSai 框架的学习实践，涉及以下知识点：

1. **AssistantAgent 使用**：单智能体创建、工具函数定义
2. **工具系统**：同步/异步工具函数、工具参数处理
3. **模型客户端**：HepAIChatCompletionClient 配置和使用
4. **回复函数**：自定义 reply_function、tool_call_summary_prompt
5. **后端服务**：run_worker、run_console 模式
6. **状态管理**：数据库管理、线程管理
7. **消息流处理**：工具调用结果处理、Markdown 输出

## 版本历史

- **v1.0** (2025-01) - 初始版本，独立的数据获取、分析、图表生成智能体
- **v2.0** (2025-02) - 整合为单一智能体，优化工具输出格式

## 参考资料

- [DrSai 框架文档](https://github.com/hepai-lab/drsai)
- [AutoGen 文档](https://microsoft.github.io/autogen)
- [HepAI 平台](https://aiapi.ihep.ac.cn)

## 许可证

本项目仅用于学习交流目的。

---

**作者**: muad.dib.lin@gmail.com
**基于**: [DrSai](https://github.com/hepai-lab/drsai) 框架
**学习项目**: 基于 DrSai 的代码学习后的初步尝试
