"""
数据查询路由模块

本模块提供了数据查询的 API 路由，包括：
- 统计数据查询（根据表名和时间范围查询统计数据）
- 交易日查询（查询指定时间范围内的交易日）

核心概念
--------

- **统计数据查询**：查询数据库中的统计数据，支持分页和排序
- **交易日查询**：查询指定时间范围内的交易日列表

为什么需要这个模块？
-------------------

在 Web 应用中，需要提供 API 接口来查询数据：
- 用户需要查询统计数据
- 用户需要查询交易日信息

这个模块提供了这些 API 接口。

工作原理（简单理解）
------------------

就像数据查询中心：

1. **接收查询请求**：接收查询请求（就像接收查询订单）
2. **执行查询**：调用服务层执行查询（就像查找数据）
3. **返回结果**：返回查询结果（就像返回数据）

注意事项
--------

- 统计数据查询支持分页和排序
- 交易日查询返回指定时间范围内的所有交易日
"""

from fastapi import APIRouter, Query

from panda_common.config import config
from panda_data_hub.services.query.stock_statistic_service import StockStatisticQuery

router = APIRouter()

@router.get('/data_query')
async def data_query(tables_name: str,
                     start_date: str,
                     end_date: str,
                     page: int = Query(default=1, ge=1, description="页码"),
                     page_size: int = Query(default=10, ge=1, le=100, description="每页数量"),
                     sort_field: str = Query(default="created_at", description="排序字段，支持created_at、return_ratio、sharpe_ratio、maximum_drawdown、IC、IR"),
                     sort_order: str = Query(default="desc", description="排序方式，asc升序，desc降序")
                     ):
    """根据表名和起止时间获取统计数据

    这个接口就像一个"数据查询器"，它会根据表名和时间范围查询统计数据，
    支持分页和排序。

    为什么需要这个接口？
    --------------------

    用户需要查询数据库中的统计数据：
    - 需要根据表名查询数据
    - 需要指定时间范围
    - 需要分页浏览，避免一次性加载太多数据
    - 需要按不同条件排序

    这个接口提供了这些功能。

    Args:
        tables_name: 表名，指定要查询的数据表
        start_date: 开始日期，格式 YYYY-MM-DD
        end_date: 结束日期，格式 YYYY-MM-DD
        page: 页码，从1开始，默认1
        page_size: 每页数量，默认10，最大100
        sort_field: 排序字段，支持 created_at、return_ratio、sharpe_ratio、maximum_drawdown、IC、IR
        sort_order: 排序方式，'asc' 表示升序，'desc' 表示降序（默认）

    Returns:
        dict: 包含统计数据列表和分页信息的字典

    Example:
        >>> GET /data_query?tables_name=stock_market&start_date=2024-01-01&end_date=2024-12-31&page=1&page_size=10
    """
    service = StockStatisticQuery(config)
    result_data = service.get_stock_statistic(tables_name, start_date, end_date, page, page_size, sort_field, sort_order)

    return result_data

@router.get('/get_trading_days')
async def get_trading_days(
        start_date: str,
        end_date: str,
):
    """获取交易日列表

    这个接口用于查询指定时间范围内的所有交易日。

    为什么需要这个接口？
    --------------------

    在量化分析中，经常需要知道交易日信息：
    - 计算因子时需要知道交易日
    - 回测时需要知道交易日
    - 数据清洗时需要跳过非交易日

    这个接口提供了查询交易日的能力。

    Args:
        start_date: 开始日期，格式 YYYY-MM-DD
        end_date: 结束日期，格式 YYYY-MM-DD

    Returns:
        dict: 包含交易日列表的字典

    Example:
        >>> GET /get_trading_days?start_date=2024-01-01&end_date=2024-12-31
    """
    service = StockStatisticQuery(config)
    result_data = service.get_trading_days(start_date, end_date)
    return result_data




