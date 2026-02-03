import sys
sys.path.insert(0, '/data/juno/lin/agent/drsai-main')

from my_agent.stock_monitor.python.data_analysis_agent import create_analysis_agent
import asyncio

async def test():
    agent = create_analysis_agent()
    # 直接调用工具测试
    from my_agent.stock_monitor.python.data_analysis_agent import get_basic_stats
    result = get_basic_stats(limit=1000, time_filter='昨天')
    print('=== Tool Result ===')
    print(result[:500])

asyncio.run(test())
