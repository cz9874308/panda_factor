"""
因子数据清洗路由模块

本模块提供了因子数据清洗的 API 路由，支持多个数据源（RiceQuant、Tushare等）。
因子数据清洗任务会在后台运行，不会阻塞请求。

核心概念
--------

- **因子数据清洗**：从外部数据源获取因子数据，进行清洗、转换和存储
- **后台任务**：数据清洗是长时间运行的任务，使用 FastAPI 的 BackgroundTasks 执行
- **进度查询**：支持查询数据清洗的进度

为什么需要这个模块？
-------------------

在 Web 应用中，需要提供 API 接口来触发因子数据清洗：
- 用户需要清洗指定时间范围的因子数据
- 数据清洗是长时间运行的任务，需要在后台执行
- 用户需要查询清洗进度

这个模块提供了这些 API 接口。

工作原理（简单理解）
------------------

就像工厂的订单系统：

1. **接收订单**：接收因子数据清洗请求（就像接收订单）
2. **启动任务**：在后台启动数据清洗任务（就像安排生产）
3. **返回确认**：立即返回任务已启动的确认（就像确认订单）
4. **查询进度**：用户可以查询任务进度（就像查询生产进度）

注意事项
--------

- 数据清洗是长时间运行的任务，使用后台任务执行
- 清洗过程可能较长，建议使用进度查询接口查看进度
- 数据源由 config 中的 DATAHUBSOURCE 配置决定
"""

from fastapi import APIRouter, BackgroundTasks
from panda_common.config import config
from typing import Dict

from panda_data_hub.services.rq_factor_clean_pro_service import FactorCleanerProService
from panda_data_hub.services.ts_factor_clean_pro_service import FactorCleanerTSProService
# from panda_data_hub.services.xt_factor_clean_pro_service import FactorCleanerXTProService

router = APIRouter()

@router.get('/upsert_factor_final')
async def upsert_factor(start_date: str, end_date: str, background_tasks: BackgroundTasks):
    """启动因子数据清洗任务

    这个接口就像一个"因子数据清洗启动器"，它会根据配置的数据源启动相应的因子数据清洗任务。
    任务会在后台运行，不会阻塞请求。

    为什么需要这个接口？
    --------------------

    用户需要清洗因子数据：
    - 需要清洗指定时间范围的因子数据
    - 数据清洗是长时间运行的任务，需要在后台执行
    - 需要支持多个数据源（RiceQuant、Tushare等）

    这个接口提供了启动因子数据清洗任务的能力。

    工作原理
    --------

    1. 重置进度为0
    2. 根据配置的数据源选择相应的服务
    3. 设置进度回调函数
    4. 在后台启动因子数据清洗任务
    5. 立即返回任务已启动的确认

    Args:
        start_date: 开始日期，格式 YYYY-MM-DD，如 "2024-01-01"
        end_date: 结束日期，格式 YYYY-MM-DD，如 "2024-12-31"
        background_tasks: FastAPI 的后台任务管理器

    Returns:
        dict: 包含任务已启动消息的字典

    Example:
        >>> GET /upsert_factor_final?start_date=2024-01-01&end_date=2024-12-31
    """
    global current_progress
    current_progress = 0  # 重置进度

    data_source = config['DATAHUBSOURCE']

    def progress_callback(progress: int):
        global current_progress
        current_progress = progress

    if data_source == 'ricequant':
        rice_quant_service = FactorCleanerProService(config)
        rice_quant_service.set_progress_callback(progress_callback)
        background_tasks.add_task(
            rice_quant_service.clean_history_data,
            start_date,
            end_date
        )
    elif data_source == 'tushare':
        tushare_service = FactorCleanerTSProService(config)
        tushare_service.set_progress_callback(progress_callback)
        background_tasks.add_task(
            tushare_service.clean_history_data,
           start_date,
            end_date)
    # elif data_source == 'xuntou':
    #     xt_quant_service = FactorCleanerXTProService(config)
    #     xt_quant_service.set_progress_callback(progress_callback)
    #     background_tasks.add_task(
    #         xt_quant_service.factor_history_clean,
    #         start_date,
    #         end_date
    #     )


    return {"message": "Factor data cleaning started by {data_source}"}

@router.get('/get_progress_factor_final')
async def get_progress() -> Dict[str, int]:
    """获取当前数据清洗进度"""
    return {"progress": current_progress}

