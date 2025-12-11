"""
用户因子路由模块

本模块提供了用户因子相关的所有 API 路由，包括：
- 因子列表查询（支持分页、排序）
- 因子 CRUD 操作（创建、查询、更新、删除）
- 因子分析（运行分析、查询状态、查询结果）
- 图表数据查询（各种分析图表的数据接口）

核心概念
--------

- **因子管理**：用户因子的创建、查询、更新、删除
- **因子分析**：运行因子分析任务，评估因子有效性
- **图表数据**：为前端图表提供数据，包括收益率、IC、分组分析等

为什么需要这个模块？
-------------------

在 Web 应用中，前端需要与后端交互：
- 用户需要创建和管理自己的因子
- 用户需要运行因子分析并查看结果
- 用户需要查看各种分析图表

这个模块提供了完整的 API 接口，满足前端的各种需求。

工作原理（简单理解）
------------------

就像餐厅的服务员：

1. **接收请求**：接收前端发来的 HTTP 请求（就像接收点餐）
2. **调用服务**：调用相应的服务函数处理业务逻辑（就像通知厨房）
3. **返回结果**：将处理结果以 JSON 格式返回（就像上菜）

注意事项
--------

- 所有路由都是异步函数，提高并发性能
- 使用 FastAPI 的 Query 参数进行参数验证
- 错误处理由服务层统一处理，路由层只负责转发
"""

from fastapi import APIRouter, Query
from panda_factor_server.services.user_factor_service import *

# 数据库处理器，用于数据库操作（虽然在这个文件中未直接使用，但保留以备将来使用）
_db_handler = DatabaseHandler(config)

# 创建 FastAPI 路由器，用于注册所有路由
router = APIRouter()


@router.get("/hello")
async def hello_route():
    """测试接口

    这个接口用于测试服务器是否正常运行。

    Returns:
        dict: 包含问候信息的字典

    Example:
        >>> GET /hello
        >>> {"message": "Hello, World!"}
    """
    return hello()

@router.get("/user_factor_list")
async def user_factor_list_route(
    user_id: str,
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=10, ge=1, le=100, description="每页数量"),
    sort_field: str = Query(default="created_at", description="排序字段，支持updated_at、created_at、return_ratio、sharpe_ratio、maximum_drawdown、IC、IR"),
    sort_order: str = Query(default="desc", description="排序方式，asc升序，desc降序")
):
    """获取用户因子列表

    这个接口就像一个"因子目录"，它会返回指定用户的所有因子列表，
    支持分页和排序，方便用户浏览和管理自己的因子。

    为什么需要这个接口？
    --------------------

    用户可能有多个因子，需要：
    - 查看所有因子
    - 按不同条件排序（如按创建时间、收益率、IC等）
    - 分页浏览，避免一次性加载太多数据

    这个接口提供了这些功能。

    工作原理
    --------

    1. 接收用户ID和分页参数
    2. 从数据库查询该用户的所有因子
    3. 根据排序字段和排序方式排序
    4. 分页返回结果

    Args:
        user_id: 用户ID，用于查询该用户的因子
        page: 页码，从1开始，默认1
        page_size: 每页数量，默认10，最大100
        sort_field: 排序字段，支持：
            - updated_at: 更新时间
            - created_at: 创建时间
            - return_ratio: 收益率
            - sharpe_ratio: 夏普比率
            - maximum_drawdown: 最大回撤
            - IC: 信息系数
            - IR: 信息比率
        sort_order: 排序方式，'asc' 表示升序，'desc' 表示降序（默认）

    Returns:
        dict: 包含因子列表和分页信息的字典，格式：
            {
                "data": [...],  # 因子列表
                "total": 100,   # 总数量
                "page": 1,      # 当前页码
                "page_size": 10 # 每页数量
            }

    Example:
        >>> GET /user_factor_list?user_id=123&page=1&page_size=10&sort_field=return_ratio&sort_order=desc
    """
    return get_user_factor_list(user_id, page, page_size, sort_field, sort_order)

@router.post("/create_factor")
async def create_factor_route(factor: CreateFactorRequest):
    """创建因子

    这个接口就像一个"因子工厂"，它会根据用户提供的因子定义创建新因子。

    为什么需要这个接口？
    --------------------

    用户需要创建自己的因子：
    - 定义因子代码（公式或Python类）
    - 设置因子参数（股票池、调仓周期等）
    - 保存因子定义

    这个接口提供了创建因子的能力。

    Args:
        factor: 因子创建请求对象，包含：
            - factor_name: 因子名称
            - code: 因子代码（公式或Python类）
            - code_type: 代码类型（'formula' 或 'python'）
            - params: 因子参数（股票池、调仓周期等）

    Returns:
        dict: 创建结果，包含因子ID和状态信息

    Example:
        >>> POST /create_factor
        >>> {
        ...     "factor_name": "my_factor",
        ...     "code": "close / open - 1",
        ...     "code_type": "formula",
        ...     "params": {...}
        ... }
    """
    return create_factor(factor)

@router.get("/delete_factor")
async def delete_user_factor_route(factor_id: str):
    """删除因子

    这个接口用于删除指定的因子。

    Args:
        factor_id: 因子ID

    Returns:
        dict: 删除结果，包含成功或失败信息

    Example:
        >>> GET /delete_factor?factor_id=123
    """
    return delete_factor(factor_id)

@router.post("/update_factor")
async def update_factor_route(factor: CreateFactorRequest, factor_id: str):
    """更新因子

    这个接口用于更新已存在的因子定义。

    Args:
        factor: 因子更新请求对象，包含新的因子定义
        factor_id: 因子ID

    Returns:
        dict: 更新结果，包含成功或失败信息

    Example:
        >>> POST /update_factor?factor_id=123
        >>> {
        ...     "factor_name": "updated_factor",
        ...     "code": "updated code",
        ...     ...
        ... }
    """
    return update_factor(factor, factor_id)

@router.get("/query_factor")
async def query_factor_route(factor_id: str):
    """查询因子详情

    这个接口用于查询指定因子的详细信息。

    Args:
        factor_id: 因子ID

    Returns:
        dict: 因子详细信息，包括因子定义、参数等

    Example:
        >>> GET /query_factor?factor_id=123
    """
    return query_factor(factor_id)
@router.get("/query_factor_status")
async def query_factor_status_route(factor_id: str):
    """查询因子状态

    这个接口用于查询因子的当前状态（如是否正在分析、分析是否完成等）。

    Args:
        factor_id: 因子ID

    Returns:
        dict: 因子状态信息，包括状态码和状态描述

    Example:
        >>> GET /query_factor_status?factor_id=123
    """
    return query_factor_status(factor_id)

@router.get("/run_factor")
async def run_factor_route(factor_id: str):
    """运行因子分析

    这个接口就像一个"分析启动器"，它会启动因子分析任务。
    分析会在后台线程中运行，不会阻塞请求。

    为什么需要这个接口？
    --------------------

    用户创建因子后，需要运行分析来评估因子的有效性：
    - 分析可能需要较长时间，不适合同步等待
    - 使用后台线程运行，可以立即返回任务ID
    - 用户可以通过任务ID查询分析进度和结果

    Args:
        factor_id: 因子ID

    Returns:
        dict: 包含任务ID和状态信息的字典

    Example:
        >>> GET /run_factor?factor_id=123
        >>> {"task_id": "task_456", "status": "started"}
    """
    return run_factor(factor_id, is_thread=True)

@router.get("/query_task_status")
async def query_task_status_route(task_id: str):
    """查询任务状态

    这个接口用于查询因子分析任务的状态和进度。

    Args:
        task_id: 任务ID

    Returns:
        dict: 任务状态信息，包括：
            - process_status: 处理状态（0-9表示不同阶段，-1表示失败）
            - updated_at: 更新时间
            - error_message: 错误信息（如果有）

    Example:
        >>> GET /query_task_status?task_id=task_456
    """
    return query_task_status(task_id)

@router.get("/query_factor_excess_chart")
async def query_factor_excess_chart_route(task_id: str):
    """查询因子超额收益图表数据

    这个接口用于获取因子相对于基准的超额收益图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: 图表数据，包含时间序列和超额收益数据

    Example:
        >>> GET /query_factor_excess_chart?task_id=task_456
    """
    return query_factor_excess_chart(task_id)

@router.get("/query_factor_analysis_data")
async def query_factor_analysis_data_route(task_id: str):
    """查询因子分析数据

    这个接口用于获取因子分析的汇总数据，包括各种性能指标。

    Args:
        task_id: 任务ID

    Returns:
        dict: 因子分析数据，包括收益率、IC、IR等指标

    Example:
        >>> GET /query_factor_analysis_data?task_id=task_456
    """
    return query_factor_analysis_data(task_id)

@router.get("/query_group_return_analysis")
async def query_group_return_analysis_route(task_id: str):
    """查询分组收益分析数据

    这个接口用于获取因子分组后的各组收益分析数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: 分组收益分析数据，包含各组的收益率、累计收益等

    Example:
        >>> GET /query_group_return_analysis?task_id=task_456
    """
    return query_group_return_analysis(task_id)

@router.get("/query_ic_decay_chart")
async def query_ic_decay_chart_route(task_id: str):
    """查询IC衰减图表数据

    这个接口用于获取IC（信息系数）随时间的衰减情况图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: IC衰减图表数据

    Example:
        >>> GET /query_ic_decay_chart?task_id=task_456
    """
    return query_ic_decay_chart(task_id)

@router.get("/query_ic_density_chart")
async def query_ic_density_chart_route(task_id: str):
    """查询IC密度分布图表数据

    这个接口用于获取IC值的分布密度图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: IC密度分布图表数据

    Example:
        >>> GET /query_ic_density_chart?task_id=task_456
    """
    return query_ic_density_chart(task_id)

@router.get("/query_ic_self_correlation_chart")
async def query_ic_self_correlation_chart_route(task_id: str):
    """查询IC自相关图表数据

    这个接口用于获取IC值的自相关图表数据，用于分析IC的稳定性。

    Args:
        task_id: 任务ID

    Returns:
        dict: IC自相关图表数据

    Example:
        >>> GET /query_ic_self_correlation_chart?task_id=task_456
    """
    return query_ic_self_correlation_chart(task_id)

@router.get("/query_ic_sequence_chart")
async def query_ic_sequence_chart_route(task_id: str):
    """查询IC序列图表数据

    这个接口用于获取IC值的时间序列图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: IC序列图表数据

    Example:
        >>> GET /query_ic_sequence_chart?task_id=task_456
    """
    return query_ic_sequence_chart(task_id)

@router.get("/query_last_date_top_factor")
async def query_last_date_top_factor_route(task_id: str):
    """查询最新日期Top因子数据

    这个接口用于获取最新日期的因子值排名前N的股票数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: Top因子数据，包含股票代码、名称、因子值等

    Example:
        >>> GET /query_last_date_top_factor?task_id=task_456
    """
    return query_last_date_top_factor(task_id)

@router.get("/query_one_group_data")
async def query_one_group_data_route(task_id: str):
    """查询单个分组数据

    这个接口用于获取指定分组的详细数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: 单个分组的数据，包含该组的所有股票信息

    Example:
        >>> GET /query_one_group_data?task_id=task_456
    """
    return query_one_group_data(task_id)

@router.get("/query_rank_ic_decay_chart")
async def query_rank_ic_decay_chart_route(task_id: str):
    """查询Rank IC衰减图表数据

    这个接口用于获取Rank IC（排名信息系数）随时间的衰减情况图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: Rank IC衰减图表数据

    Example:
        >>> GET /query_rank_ic_decay_chart?task_id=task_456
    """
    return query_rank_ic_decay_chart(task_id)

@router.get("/query_rank_ic_density_chart")
async def query_rank_ic_density_chart_route(task_id: str):
    """查询Rank IC密度分布图表数据

    这个接口用于获取Rank IC值的分布密度图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: Rank IC密度分布图表数据

    Example:
        >>> GET /query_rank_ic_density_chart?task_id=task_456
    """
    return query_rank_ic_density_chart(task_id)

@router.get("/query_rank_ic_self_correlation_chart")
async def query_rank_ic_self_correlation_chart_route(task_id: str):
    """查询Rank IC自相关图表数据

    这个接口用于获取Rank IC值的自相关图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: Rank IC自相关图表数据

    Example:
        >>> GET /query_rank_ic_self_correlation_chart?task_id=task_456
    """
    return query_rank_ic_self_correlation_chart(task_id)

@router.get("/query_rank_ic_sequence_chart")
async def query_rank_ic_sequence_chart_route(task_id: str):
    """查询Rank IC序列图表数据

    这个接口用于获取Rank IC值的时间序列图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: Rank IC序列图表数据

    Example:
        >>> GET /query_rank_ic_sequence_chart?task_id=task_456
    """
    return query_rank_ic_sequence_chart(task_id)

@router.get("/query_return_chart")
async def query_return_chart_route(task_id: str):
    """查询收益图表数据

    这个接口用于获取因子各组的累计收益图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: 收益图表数据，包含各组的累计收益曲线

    Example:
        >>> GET /query_return_chart?task_id=task_456
    """
    return query_return_chart(task_id)

@router.get("/query_simple_return_chart")
async def query_simple_return_chart_route(task_id: str):
    """查询简单收益图表数据

    这个接口用于获取因子各组的简单收益（非累计）图表数据。

    Args:
        task_id: 任务ID

    Returns:
        dict: 简单收益图表数据

    Example:
        >>> GET /query_simple_return_chart?task_id=task_456
    """
    return query_simple_return_chart(task_id)

@router.get("/task_logs")
async def get_task_logs_route(task_id: str, last_log_id: str = None):
    """获取任务日志

    这个接口用于获取因子分析任务的实时日志，支持增量获取。

    为什么需要这个接口？
    --------------------

    在因子分析过程中，用户需要查看分析进度和日志：
    - 分析过程可能较长，需要实时查看进度
    - 如果分析出错，需要查看错误日志
    - 支持增量获取，避免重复获取已读日志

    Args:
        task_id: 任务ID
        last_log_id: 最后一条日志ID，用于增量获取（可选）

    Returns:
        dict: 任务日志列表，包含日志内容和时间戳

    Example:
        >>> GET /task_logs?task_id=task_456
        >>> GET /task_logs?task_id=task_456&last_log_id=log_123
    """
    return get_task_logs(task_id, last_log_id=last_log_id)
