"""
用户因子服务模块

本模块提供了用户因子相关的所有业务逻辑，包括：
- 因子管理（创建、查询、更新、删除）
- 因子分析（运行分析、查询状态、查询结果）
- 图表数据查询（各种分析图表的数据处理）

核心概念
--------

- **因子管理**：用户因子的 CRUD 操作和验证
- **因子分析**：运行因子分析任务，评估因子有效性
- **图表数据**：处理分析结果，为前端图表提供格式化数据

为什么需要这个模块？
-------------------

在 Web 应用中，需要将业务逻辑与路由分离：
- 路由层只负责接收请求和返回响应
- 服务层负责处理业务逻辑和数据操作
- 这样可以让代码更清晰、更易维护

这个模块统一管理所有因子相关的业务逻辑，提供了清晰的接口。

工作原理（简单理解）
------------------

就像餐厅的厨房：

1. **接收订单**：接收路由层传来的请求（就像接收点餐单）
2. **处理业务**：执行业务逻辑（就像准备菜品）
3. **返回结果**：将处理结果返回给路由层（就像出菜）

注意事项
--------

- 所有函数都是同步函数，由路由层的异步函数调用
- 错误处理使用 HTTPException 抛出，由 FastAPI 统一处理
- 数据库操作使用全局的 _db_handler 实例
"""

import numpy as np
import logging


from panda_common.handlers.database_handler import DatabaseHandler
import panda_data
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from bson import ObjectId
import traceback
from panda_common.handlers.log_handler import get_factor_logger
from panda_factor.analysis.factor_analysis import factor_analysis
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_factor.generate.macro_factor import MacroFactor
from ..models.request_body import *
from ..models.response_body import UserFactorDetailResponse, TaskResult, FactorExcessChartResponse, FactorAnalysisDataResponse, GroupReturnAnalysisResponse, ICDecayChartResponse, ICDensityChartResponse, ICSelfCorrelationChartResponse, ICSequenceChartResponse, LastDateTopFactorResponse, OneGroupDataResponse, RankICDecayChartResponse, RankICDensityChartResponse, RankICSelfCorrelationChartResponse, RankICSequenceChartResponse, ReturnChartResponse, SimpleReturnChartResponse, UserFactorListResponse, UserFactorListItem
from ..models.result_data import *
from panda_common.config import config
from panda_common.models.factor_analysis_params import Params
from typing import Tuple, Optional

# 全局变量，替代类实例变量
# 使用全局变量而不是类实例，简化函数调用，避免需要创建服务类实例
_config = config  # 配置对象
_db_handler = DatabaseHandler(config)  # 数据库处理器
panda_data.init()  # 初始化数据读取模块

def validate_object_id(factor_id: str) -> ObjectId:
    """验证并转换ObjectId

    这个函数用于验证因子ID的格式是否正确，并转换为 MongoDB 的 ObjectId 对象。

    为什么需要这个函数？
    --------------------

    MongoDB 使用 ObjectId 作为文档的唯一标识符。
    如果用户传入的因子ID格式不正确，会导致查询失败。
    这个函数提前验证格式，提供更友好的错误信息。

    Args:
        factor_id: 因子ID字符串

    Returns:
        ObjectId: MongoDB ObjectId 对象

    Raises:
        HTTPException: 如果因子ID格式无效，返回400错误

    Example:
        >>> obj_id = validate_object_id("507f1f77bcf86cd799439011")
    """
    try:
        return ObjectId(factor_id)
    except Exception:
        logger.warning(f"Invalid ObjectId format: {factor_id}")
        raise HTTPException(status_code=400, detail="无效的因子ID格式")

def check_factor_exists(user_id: str, factor_name: str, exclude_id: str = None) -> bool:
    """检查因子是否存在

    这个函数用于检查指定用户是否已经存在同名的因子。

    为什么需要这个函数？
    --------------------

    在创建或更新因子时，需要确保因子名称不重复：
    - 同一用户不能有重复的因子名称
    - 更新因子时可以排除当前因子ID

    这个函数提供了检查因子是否存在的功能。

    Args:
        user_id: 用户ID
        factor_name: 因子名称
        exclude_id: 要排除的因子ID（用于更新时检查，可选）

    Returns:
        bool: 如果因子存在返回 True，否则返回 False

    Example:
        >>> exists = check_factor_exists("user_123", "my_factor")
    """
    query = {"user_id": user_id, "factor_name": factor_name}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}  # 排除指定ID
    return bool(_db_handler.mongo_find_one("panda", "user_factors", query))


def format_duration(seconds):
    """将秒数格式化为可读的时间格式

    这个函数将秒数转换为人类可读的时间格式，如 "1小时30分钟20秒"。

    为什么需要这个函数？
    --------------------

    在显示任务执行时间时，直接显示秒数不够直观。
    这个函数将秒数转换为更易读的格式，提升用户体验。

    Args:
        seconds: 秒数（可以是浮点数）

    Returns:
        str: 格式化后的时间字符串，如 "1小时30分钟20秒"

    Example:
        >>> format_duration(3661)
        '1小时1分钟1秒'
        >>> format_duration(30)
        '30秒'
    """
    if seconds < 0:
        return "0秒"

    # 将秒数转换为小时、分钟、秒
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    result = []
    if hours > 0:
        result.append(f"{int(hours)}小时")
    if minutes > 0:
        result.append(f"{int(minutes)}分钟")
    if seconds > 0 or not result:  # 如果没有小时和分钟，至少显示秒
        result.append(f"{int(seconds)}秒")

    return "".join(result)


def hello():
    """测试函数

    这个函数用于测试服务是否正常运行。

    Returns:
        ResultData: 包含 "hello" 消息的成功结果

    Example:
        >>> result = hello()
    """
    return ResultData.success("hello")


def get_user_factor_list(
    user_id: str,
    page: int = 1,
    page_size: int = 10,
    sort_field: str = "created_at",
    sort_order: str = "desc"
):
    """获取用户因子列表

    这个函数就像一个"因子目录管理器"，它会查询指定用户的所有因子，
    支持分页和排序，并包含每个因子的性能指标。

    为什么需要这个函数？
    --------------------

    用户可能有多个因子，需要：
    - 查看所有因子
    - 按不同条件排序（如按创建时间、收益率、IC等）
    - 分页浏览，避免一次性加载太多数据
    - 查看每个因子的性能指标（收益率、IC、IR等）

    这个函数提供了这些功能。

    工作原理
    --------

    1. 验证排序参数
    2. 查询用户的所有因子
    3. 获取总记录数，计算总页数
    4. 根据排序字段和排序方式排序
    5. 分页查询数据
    6. 为每个因子查询最新的分析结果，获取性能指标
    7. 格式化返回结果

    Args:
        user_id: 用户ID，用于查询该用户的因子
        page: 页码，从1开始，默认1
        page_size: 每页数量，默认10，最大100
        sort_field: 排序字段，支持：
            - created_at: 创建时间
            - return_ratio: 收益率
            - sharpe_ratio: 夏普比率
            - maximum_drawdown: 最大回撤
            - IC: 信息系数
            - IR: 信息比率
        sort_order: 排序方式，'asc' 表示升序，'desc' 表示降序（默认）

    Returns:
        ResultData: 包含因子列表和分页信息的结果对象

    Raises:
        HTTPException: 如果排序参数无效，返回400错误

    Example:
        >>> result = get_user_factor_list(
        ...     user_id="user_123",
        ...     page=1,
        ...     page_size=10,
        ...     sort_field="return_ratio",
        ...     sort_order="desc"
        ... )
    """
    try:
        # 验证排序参数
        valid_sort_fields = ["created_at","created_at", "return_ratio", "sharpe_ratio", "maximum_drawdown", "IC", "IR"]
        if sort_field not in valid_sort_fields:
            raise HTTPException(status_code=400, detail=f"不支持的排序字段: {sort_field}")

        if sort_order not in ["asc", "desc"]:
            raise HTTPException(status_code=400, detail=f"不支持的排序方式: {sort_order}")

        # 查询用户因子基本信息
        query = {"user_id": user_id}

        # 获取总记录数
        total = _db_handler.mongo_client["panda"]["user_factors"].count_documents(query)

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size

        # 如果请求的页码超过总页数，返回空列表
        if page > total_pages and total_pages > 0:
            return UserFactorListResponse(
                data=[],
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages
            )

        # 计算跳过的记录数
        skip = (page - 1) * page_size

        # 查询当前页的数据
        cursor = _db_handler.mongo_client["panda"]["user_factors"].find(query)

        # 将游标转换为列表
        factor_list = list(cursor)

        if not factor_list:
            logger.info(f"未找到用户 {user_id} 的因子")
            return UserFactorListResponse(
                data=[],
                total=0,
                page=page,
                page_size=page_size,
                total_pages=0
            )

        # 处理每个因子的数据
        result_list = []
        for factor in factor_list:
            # 获取基本信息
            factor_info = {
                "name": factor["name"],
                "factor_id": str(factor["_id"]),
                "factor_name": factor.get("factor_name", ""),
                "updated_at": factor.get("updated_at", ""),
                "created_at": factor.get("created_at", ""),
                "return_ratio": "0.0%",
                "sharpe_ratio": 0.0,
                "maximum_drawdown": "0.0%",
                "annualized_ratio":"0.0%",
                "IC": 0.0000,
                "IR": 0.0000
            }

            # 获取最新的task_id
            task_id = factor.get("current_task_id")
            if task_id:
                # 查询性能指标
                analysis_result = _db_handler.mongo_find_one(
                    "panda",
                    "factor_analysis_results",
                    {"task_id": task_id}
                )

                if analysis_result:
                    # 获取单组数据分析结果
                    if "one_group_data" in analysis_result:
                        one_group_data = analysis_result["one_group_data"]
                        return_ratio = one_group_data["return_ratio"]
                        annualized_ratio = one_group_data["annualized_ratio"]
                        sharpe_ratio = one_group_data["sharpe_ratio"]
                        maximum_drawdown = one_group_data["maximum_drawdown"]
                        factor_info.update({
                            "return_ratio": return_ratio ,
                            "annualized_ratio": annualized_ratio ,
                            "sharpe_ratio": sharpe_ratio ,
                            "maximum_drawdown": maximum_drawdown ,
                        })

                    # 获取因子分析数据中的IC和IR
                    if "factor_data_analysis" in analysis_result:

                        for item in analysis_result["factor_data_analysis"]:
                            if item["指标"] == "IC_mean":
                                # factor_info["IC"] = round(float(item[list(item.keys())[1]]) if item[list(item.keys())[1]] != "-" else 0.0, 4)
                                factor_info["IC"] = round(float(item[list(item.keys())[1]]),4)
                            elif item["指标"] == "IC_IR":
                                # factor_info["IR"] = round(float(item[list(item.keys())[1]]) if item[list(item.keys())[1]] != "-" else 0.0, 4)
                                factor_info["IR"] =  round(float(item[list(item.keys())[1]]),4)

            result_list.append(UserFactorListItem(**factor_info))

        # 对结果列表进行排序
        reverse = sort_order == "desc"
        result_list.sort(key=lambda x: getattr(x, sort_field), reverse=reverse)

        # 应用分页
        start_idx = skip
        end_idx = min(start_idx + page_size, len(result_list))
        result_list = result_list[start_idx:end_idx]

        logger.info(
            f"成功获取用户 {user_id} 的第 {page} 页因子信息，每页 {page_size} 条，按 {sort_field} {'降序' if reverse else '升序'} 排序")
        return ResultData.success(data=UserFactorListResponse(
            data=result_list,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        ))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户 {user_id} 的因子列表失败: {str(e)}\n{traceback.format_exc()}")
        # raise HTTPException(status_code=500, detail=f"获取因子列表失败: {str(e)}")
        return ResultData.fail("500", f"获取因子列表失败: {str(e)}")


def create_factor(factor: CreateFactorRequest):
    """创建因子

    这个函数就像一个"因子工厂"，它会根据用户提供的因子定义创建新因子并保存到数据库。

    为什么需要这个函数？
    --------------------

    用户需要创建自己的因子：
    - 定义因子代码（公式或Python类）
    - 设置因子参数（股票池、调仓周期等）
    - 保存因子定义到数据库

    这个函数提供了创建因子的完整流程。

    工作原理
    --------

    1. 检查因子名称是否已存在（同一用户不能有重复的因子名称）
    2. 准备因子数据，添加创建时间和更新时间
    3. 将因子保存到数据库
    4. 返回创建结果和因子ID

    Args:
        factor: 因子创建请求对象，包含：
            - user_id: 用户ID
            - factor_name: 因子名称
            - code: 因子代码（公式或Python类）
            - code_type: 代码类型（'formula' 或 'python'）
            - params: 因子参数

    Returns:
        ResultData: 创建结果，包含因子ID或错误信息

    Example:
        >>> factor_request = CreateFactorRequest(
        ...     user_id="user_123",
        ...     factor_name="my_factor",
        ...     code="close / open - 1",
        ...     code_type="formula",
        ...     params={...}
        ... )
        >>> result = create_factor(factor_request)
    """
    try:
        # 检查因子是否已存在（同一用户不能有重复的因子名称）
        if check_factor_exists(factor.user_id, factor.factor_name):
            return ResultData.fail("409", "同名因子已存在")

        # 准备数据：将请求对象转换为字典，并添加时间戳
        factor_dict = factor.dict()
        factor_dict.update({
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })

        # 创建因子：将因子数据插入到数据库
        result = _db_handler.mongo_insert_many("panda", "user_factors", [factor_dict])

        if result and len(result) > 0:
            factor_dict["_id"] = str(result[0])
            logger.info(f"Successfully created user factor: {factor.factor_name}")
            return ResultData.success(message="因子创建成功", data={"factor_id": str(result[0])})

        return ResultData.fail("500", "因子创建失败")

    except Exception as e:
        logger.error(f"Failed to create user factor: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"创建因子失败: {str(e)}")

def delete_factor(factor_id: str):
    """删除因子

    这个函数用于删除指定的因子。

    Args:
        factor_id: 因子ID

    Returns:
        ResultData: 删除结果，包含成功或失败信息

    Raises:
        HTTPException: 如果因子ID格式无效，返回400错误

    Example:
        >>> result = delete_factor("507f1f77bcf86cd799439011")
    """
    try:
        # 验证并转换因子ID
        object_id = validate_object_id(factor_id)

        # 从数据库删除因子
        result = _db_handler.mongo_delete(
            "panda",
            "user_factors",
            {"_id": object_id}
        )

        if result:
            logger.info(f"Successfully deleted user factor with ID: {factor_id}")
            return ResultData.success(message="因子删除成功", data={"factor_id": factor_id})

        logger.warning(f"User factor not found with ID: {factor_id}")
        return ResultData.fail("404", "未找到用户因子")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete user factor: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"删除因子失败: {str(e)}")


def update_factor(factor: CreateFactorRequest, factor_id: str):
    """更新因子

    这个函数用于更新已存在的因子定义。

    为什么需要这个函数？
    --------------------

    用户可能需要修改已创建的因子：
    - 修改因子代码
    - 修改因子参数
    - 修改因子名称

    这个函数提供了更新因子的能力。

    工作原理
    --------

    1. 验证因子ID格式
    2. 检查因子是否存在
    3. 检查是否有其他同名因子（排除当前因子）
    4. 准备更新数据，更新修改时间
    5. 全量更新因子文档
    6. 返回更新结果

    Args:
        factor: 因子更新请求对象，包含新的因子定义
        factor_id: 因子ID

    Returns:
        ResultData: 更新结果，包含成功或失败信息

    Raises:
        HTTPException: 如果因子ID格式无效，返回400错误

    Example:
        >>> factor_request = CreateFactorRequest(...)
        >>> result = update_factor(factor_request, "507f1f77bcf86cd799439011")
    """
    try:
        # 验证并转换因子ID
        object_id = validate_object_id(factor_id)

        # 检查因子是否存在
        if not _db_handler.mongo_find_one("panda", "user_factors", {"_id": object_id}):
            logger.warning(f"Factor with ID {factor_id} not found for update")
            return ResultData.fail("404", "未找到要更新的因子")

        # 检查是否有其他同名因子（排除当前因子）
        if check_factor_exists(factor.user_id, factor.factor_name, factor_id):
            return ResultData.fail("409", "同名因子已存在")

        # 准备更新数据：将请求对象转换为字典，并更新修改时间
        factor_dict = factor.dict()
        factor_dict["updated_at"] = datetime.now().isoformat()

        # 更新因子：全量更新所有字段，使用文档替换的方式
        result = _db_handler.mongo_update(
            "panda",
            "user_factors",
            {"_id": object_id},
            factor_dict  # 直接使用文档替换，而不是使用 $set 操作符
        )

        if result:
            logger.info(f"Successfully updated user factor: {factor.factor_name}")
            return ResultData.success(message="因子更新成功", data={"factor_id": factor_id})

        logger.warning(f"No changes made to factor: {factor_id}")
        return ResultData.success(message="因子未发生变化", data={"factor_id": factor_id})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update user factor: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"更新因子失败: {str(e)}")


def query_factor(factor_id: str):
    try:
        object_id = validate_object_id(factor_id)

        # 使用 mongo_find 并获取第一个结果
        factors = _db_handler.mongo_find("panda", "user_factors", {"_id": object_id})

        if factors and len(factors) > 0:
            factor = factors[0]  # 获取第一个结果
            # 转换 ObjectId 为字符串
            factor["_id"] = str(factor["_id"])

            # 将数据映射到 UserFactorDetailResponse 模型
            factor_detail = UserFactorDetailResponse(**factor)

            logger.info(f"Successfully retrieved factor with ID: {factor_id}")
            return ResultData.success(message="获取因子成功", data=factor_detail)

        logger.warning(f"Factor not found with ID: {factor_id}")
        return ResultData.fail("404", "未找到指定因子")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query factor: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询因子失败: {str(e)}")

def query_factor_status(factor_id: str):
    try:
        object_id = validate_object_id(factor_id)

        # 使用 mongo_find 并获取第一个结果
        factors = _db_handler.mongo_find("panda", "user_factors", {"_id": object_id})

        if not factors or len(factors) == 0:
            logger.warning(f"Factor not found with ID: {factor_id}")
            return ResultData.fail("404", "未找到指定因子")

        factor = factors[0]  # 获取第一个结果
        # 只返回status字段和current_task_id字段
        status = factor.get("status", 0)
        task_id = factor.get("current_task_id", "unknown")

        logger.info(f"Successfully retrieved factor status with ID: {factor_id}")
        return ResultData.success(message="获取因子状态成功", data={"status": status, "task_id": task_id})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query factor status: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询因子状态失败: {str(e)}")

def validate_factor_params(factor: dict, logger: logging.Logger) -> Tuple[bool, str, Optional[Params]]:
    """验证因子参数

    这个函数就像一个"参数检查员"，它会验证因子参数是否合法，
    包括调仓周期、股票池、分组数量、极值处理方法等。

    为什么需要这个函数？
    --------------------

    在运行因子分析前，需要确保参数合法：
    - 调仓周期必须在支持的范围内
    - 股票池必须是有效的代码
    - 分组数量必须在合理范围内
    - 因子代码必须通过安全验证

    这个函数提供了完整的参数验证功能。

    工作原理
    --------

    1. 从因子字典中提取参数
    2. 转换为 Params 对象
    3. 验证各个参数是否合法：
       - 调仓周期：必须在 [1, 3, 5, 10, 20, 30] 中
       - 股票池：必须是 000300、000905、000852、000985 之一
       - 分组数量：必须在 2-20 之间
       - 极值处理方法：必须是"标准差"或"中位数"
       - 日期：必须有开始日期和结束日期
    4. 验证因子代码是否安全
    5. 返回验证结果

    Args:
        factor: 因子信息字典，包含 params 和 code 字段
        logger: 日志记录器，用于记录验证过程

    Returns:
        Tuple[bool, str, Optional[Params]]:
        - bool: 验证是否通过，True 表示通过，False 表示失败
        - str: 错误信息（如果验证失败），成功时为空字符串
        - Optional[Params]: 验证通过后的参数对象，失败时为 None

    Example:
        >>> factor_dict = {
        ...     "params": {
        ...         "start_date": "2024-01-01",
        ...         "end_date": "2024-12-31",
        ...         "adjustment_cycle": 5,
        ...         "stock_pool": "000300",
        ...         "group_number": 10
        ...     },
        ...     "code": "close / open - 1",
        ...     "code_type": "formula"
        ... }
        >>> is_valid, error_msg, params = validate_factor_params(factor_dict, logger)
    """
    # 从factor的param字段中获取日期
    params_dict = factor.get("params", {})

    try:
        # 转换参数字典为Params对象
        params = Params(
            start_date=params_dict.get("start_date", ""),
            end_date=params_dict.get("end_date", ""),
            adjustment_cycle=params_dict.get("adjustment_cycle", 1),
            stock_pool=params_dict.get("stock_pool", "000300"),
            factor_direction=params_dict.get("factor_direction", False),
            group_number=params_dict.get("group_number", 5),
            include_st=params_dict.get("include_st", False),
            extreme_value_processing=params_dict.get("extreme_value_processing", "中位数")
        )
        logger.debug(f"转换为Params对象成功: {params.dict()}")

        # 验证调仓周期
        valid_adjustment_cycles = [1, 3, 5, 10 ,20 ,30]
        if params.adjustment_cycle not in valid_adjustment_cycles:
            error_msg = f"不支持的调仓周期: {params.adjustment_cycle}，只支持[1, 3, 5, 10 ,20 ,30]"
            logger.error(error_msg)
            return False, error_msg, None

        # 验证股票池
        valid_stock_pools = ["000300", "000905", "000852", "000985"]
        if params.stock_pool not in valid_stock_pools:
            error_msg = f"不支持的股票池: {params.stock_pool}，只支持000300、000905、000852、000985"
            logger.error(error_msg)
            return False, error_msg, None

        # 验证分组数量
        if not (2 <= params.group_number <= 20):
            error_msg = f"分组数量必须在2到20之间，当前值: {params.group_number}"
            logger.error(error_msg)
            return False, error_msg, None

        # 验证极值处理方法
        valid_extreme_methods = ["标准差", "std", "中位数", "median"]
        if params.extreme_value_processing not in valid_extreme_methods:
            error_msg = f"不支持的极值处理方法: {params.extreme_value_processing}，只支持标准差和中位数"
            logger.error(error_msg)
            return False, error_msg, None

        # 验证日期参数
        if not params.start_date:
            error_msg = "startdate not found in factor param"
            logger.error(error_msg)
            return False, f"缺少开始日期: {error_msg}", None

        if not params.end_date:
            error_msg = "enddate not found in factor param"
            logger.error(error_msg)
            return False, f"缺少结束日期: {error_msg}", None

        # 验证因子代码
        try:
            macro_factor = MacroFactor()
            result = macro_factor.validate_factor(factor.get('code', ''), factor.get('code_type', ''))
            if not result['is_valid']:
                logger.error("Code validation failed:")
                logger.error(f"Syntax errors: {result.get('syntax_errors', 'No syntax errors')}")
                logger.error(f"Missing factors: {result.get('missing_factors', 'No missing factors')}")
                logger.error(f"Code errors: {result.get('formula_errors', 'No formula errors')}")
                return False, "Factor code validation failed, please check your code", None
            logger.debug("Code is valid")
        except Exception as e:
            error_msg = f"Factor validation failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, None

        return True, "", params

    except Exception as e:
        error_msg = f"转换参数失败: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, None

def run_factor(message_id: str, is_thread: bool):
    """运行因子分析

    这个函数就像一个"分析启动器"，它会启动因子分析任务。
    如果 is_thread=True，分析会在后台线程中运行，不会阻塞请求。

    为什么需要这个函数？
    --------------------

    用户创建因子后，需要运行分析来评估因子的有效性：
    - 分析可能需要较长时间，不适合同步等待
    - 使用后台线程运行，可以立即返回任务ID
    - 用户可以通过任务ID查询分析进度和结果

    工作原理（简单理解）
    ------------------

    就像启动一个后台任务：

    1. **解析参数**：从 message_id 中提取因子ID和任务ID（如果有）
    2. **创建任务**：在数据库中创建任务记录
    3. **更新状态**：将因子状态更新为"运行中"
    4. **验证参数**：验证因子参数是否合法
    5. **启动分析**：
       - 如果 is_thread=True：在后台线程中运行分析
       - 如果 is_thread=False：同步运行分析（会阻塞）

    Args:
        message_id: 消息ID，可以是因子ID，也可以是 "factor_id$task_id" 格式
        is_thread: 是否在后台线程中运行，True 表示后台运行，False 表示同步运行

    Returns:
        ResultData: 包含任务ID和状态信息的结果对象

    Raises:
        HTTPException: 如果因子ID格式无效或因子不存在，返回相应错误

    Example:
        >>> result = run_factor("507f1f77bcf86cd799439011", is_thread=True)
        >>> # 返回: {"task_id": "task_456", "status": 1}
    """
    try:
        print(message_id)
        # 解析 message_id：如果包含 $，则提取因子ID和任务ID；否则生成新的任务ID
        if message_id.__contains__("$"):
            factor_id = message_id.split("$")[0]
            task_id = message_id.split("$")[1]
        else:
            import uuid
            task_id = str(uuid.uuid4()).replace("-", "")  # 生成新的任务ID
            factor_id = message_id

        # 验证并转换因子ID
        object_id = validate_object_id(factor_id)
        # 生成任务ID
        # import uuid
        # task_id = str(uuid.uuid4()).replace("-", "")  # 移除UUID中的破折号
        logger = get_factor_logger(
            task_id=task_id or "unknown",
            factor_id=factor_id or "unknown"
        )
        logger.debug(f"factor_id: {factor_id}")
        # 查询因子
        factors = _db_handler.mongo_find("panda", "user_factors", {"_id": object_id})

        if not factors or len(factors) == 0:
            logger.warning(f"Factor not found with ID: {factor_id}")
            return ResultData.fail("404", "未找到指定因子")

        factor = factors[0]  # 获取第一个结果

        # 检查因子是否已经在运行中
        # if factor.get("status") == 1:
        #     logger.info(f"Factor with ID {factor_id} is already running")
        #     current_task_id = factor.get("current_task_id", "unknown")
        #     return ResultData.success(message="因子已在运行中",
        #                               data={"factor_id": factor_id, "task_id": current_task_id, "status": 1})

        # 记录因子信息
        user_id = factor.get("user_id")
        factor_name = factor.get("factor_name")
        params_dict = factor.get("params", {})
        # 创建任务记录
        task_record = {
            "task_id": task_id,
            "factor_id": factor_id,
            "user_id": user_id,
            "factor_name": factor_name,
            "task_type": "factor_analysis",
            "params": params_dict,  # 使用Params对象的dict方法获取全部参数
            "status": 1,  # 1: 运行中, 2: 完成, 3: 失败
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error_message": None,
            "result": None
        }

        # 将任务记录保存到MongoDB的tasks集合中
        _db_handler.mongo_insert("panda", "tasks", task_record)
        logger.debug(f"Created task record with ID: {task_id}")

        # 更新因子状态为运行中(status=1)
        _db_handler.mongo_update(
            "panda",
            "user_factors",
            {"_id": object_id},
            {
                "status": 1,  # 运行中
                "updated_at": datetime.now().isoformat(),
                "current_task_id": task_id,  # 保存当前任务ID
                "result": {"task_id": task_id}  # 保存任务ID在结果字段中
            }
        )
        logger.debug(f"=======Factor parameters validation =======")
        # 验证因子参数
        is_valid, error_msg, params = validate_factor_params(factor, logger)
        if not is_valid:
            return ResultData.fail("400", error_msg)
        logger.debug(f"=======Factor parameters validated successfully=======")
        # 从param中获取startdate和enddate
        start_date = params.start_date
        end_date = params.end_date

        logger.debug(
            f"准备运行因子 - user_id: {user_id}, factor_name: {factor_name}, start_date: {start_date}, end_date: {end_date}")


        if is_thread:
        # 创建后台进程运行因子分析
            import threading
        # 启动后台线程
            thread = threading.Thread(target=run_factor_analysis, args=(factor_id,start_date, end_date,user_id,factor_name,params,task_id,object_id,logger,))
            thread.daemon = True  # 设置为守护线程，主线程结束时自动退出
            thread.start()
            logger.info(f"Started factor analysis in background for ID: {factor_id}, task ID: {task_id}")
            return ResultData.success(message="因子分析已启动，正在后台运行",
                                  data={"factor_id": factor_id, "task_id": task_id, "status": 1})
        else:
            run_factor_analysis(factor_id,start_date, end_date,user_id,factor_name,params,task_id,object_id,logger)
            return ResultData.success(message="因子分析完成",data={"factor_id": factor_id, "task_id": task_id})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start factor analysis: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"启动因子分析失败: {str(e)}")

def run_factor_analysis(factor_id: str, start_date: str, end_date: str, user_id: str, factor_name: str, params: Params, task_id: str, object_id: ObjectId, logger: logging.Logger) -> None:
    """运行因子分析（实际执行函数）

    这个函数是因子分析的实际执行函数，它会在后台线程中运行。
    它会计算因子值，然后调用 factor_analysis 进行完整的分析。

    为什么需要这个函数？
    --------------------

    因子分析是一个长时间运行的任务，需要：
    - 计算因子值（可能需要较长时间）
    - 运行完整的因子分析（包括分组、回测等）
    - 更新任务状态和因子状态

    这个函数在后台线程中运行，不会阻塞主线程。

    工作原理
    --------

    1. 格式化日期（将 YYYY-MM-DD 转换为 YYYYMMDD）
    2. 初始化数据读取模块
    3. 获取因子数据（调用 get_custom_factor）
    4. 检查因子数据是否为空
    5. 运行因子分析（调用 factor_analysis）
    6. 更新任务状态为"完成"
    7. 更新因子状态为"已完成"

    Args:
        factor_id: 因子ID
        start_date: 开始日期，格式 YYYY-MM-DD 或 YYYYMMDD
        end_date: 结束日期，格式 YYYY-MM-DD 或 YYYYMMDD
        user_id: 用户ID
        factor_name: 因子名称
        params: 分析参数对象
        task_id: 任务ID
        object_id: MongoDB ObjectId 对象
        logger: 日志记录器

    Returns:
        None: 函数不返回值，结果通过更新数据库状态来反映

    Note:
        这个函数在后台线程中运行，如果出错会更新任务状态为失败
    """
    try:
        logger.debug(f"Factor analysis for ID: {factor_id}, task ID: {task_id}")

        logger.debug("======= Starting factor calculation =======")
        # 获取因子值 - 格式化日期（将 YYYY-MM-DD 转换为 YYYYMMDD）
        start_date_formatted = start_date.replace("-", "") if "-" in start_date else start_date
        end_date_formatted = end_date.replace("-", "") if "-" in end_date else end_date
        panda_data.init()  # 初始化数据读取模块
        # 获取自定义因子数据
        df_factor = panda_data.get_custom_factor(
            factor_logger=logger,
            user_id=int(user_id),
            factor_name=factor_name,
            start_date=start_date_formatted,
            end_date=end_date_formatted
        )
        print(df_factor.tail(5))
        logger.debug(f"Factor data len : {len(df_factor)}")
        logger.debug(f"=======Factor data retrieved successfully=======")
        
        # 判断因子数据是否为空
        if df_factor.empty:
            logger.error(f"Factor data is empty, please check your factor definition or date range")
            return ResultData.fail(code="400", message="Factor data is empty, please check your factor definition or date range")
        
        # 重置索引，将多级索引转换为普通列
        df_factor = df_factor.reset_index(drop=False)
        # 运行因子分析：调用 factor_analysis 函数进行完整的分析
        factor_analysis(df_factor, params, factor_id, task_id, logger)

        # 线程内部执行完成后更新状态
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "status": 2,  # 完成
                "updated_at": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "result": "分析完成"
            }
        )

        # 更新因子状态为已完成(status=2)
        _db_handler.mongo_update(
            "panda",
            "user_factors",
            {"_id": object_id},
            {
                "status": 2,  # 已完成
                "updated_at": datetime.now().isoformat(),
                "last_run_at": datetime.now().isoformat(),
                "result": {"task_id": task_id},
                "current_task_id": task_id
            }
        )
    except Exception as e:
        error_msg = f"因子分析失败: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)

        # 更新任务状态为失败
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "status": 3,  # 失败
                "updated_at": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "error_message": error_msg
            }
        )

        # 更新因子状态为失败(status=3)
        _db_handler.mongo_update(
            "panda",
            "user_factors",
            {"_id": object_id},
            {
                "status": 3,  # 失败
                "updated_at": datetime.now().isoformat(),
                "last_run_at": datetime.now().isoformat(),
                "result": {"error": error_msg}
            }
        )
def query_task_status(task_id: str):
    """
    查询任务状态接口

    参数:
    - task_id: 任务ID

    返回:
    - 任务状态信息
    """
    try:
        # 构建查询条件
        query = {"task_id": task_id}

        # 查询任务
        tasks = _db_handler.mongo_find("panda", "tasks", query)

        if not tasks:
            return ResultData.fail("404", "未找到指定任务")

        task = tasks[0]

        # 提取需要的字段
        result = TaskResult(
            process_status=task.get("process_status"),
            error_message=task.get("error_message"),
            result=task.get("result"),
            last_log_message=task.get("last_log_message"),
            last_log_time=task.get("last_log_time"),
            task_id=task.get("task_id"),
            factor_id=task.get("factor_id"),
            user_id=task.get("user_id"),
            factor_name=task.get("factor_name")
        )

        logger.info(f"Successfully queried task: {task_id}")
        return ResultData.success(data=result)
    except Exception as e:
        logger.error(f"Failed to query task: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询任务失败: {str(e)}")

def get_task_logs(task_id: str, last_log_id: str = None):
    """
    获取任务日志
    :param task_id: 任务ID
    :param last_log_id: 上次获取的最后一个日志ID，用于增量获取
    :return: 日志消息列表，每个元素包含message、loglevel和timestamp
    """
    try:
        _db_handler = DatabaseHandler(config)

        # 构建查询条件
        query = {"task_id": task_id}
        if last_log_id:
            # 直接查询_id大于last_log_id的文档
            query["_id"] = {"$gt": ObjectId(last_log_id)}

        # 查询日志并按时间戳排序
        logs = _db_handler.mongo_find(
            "panda",
            "factor_analysis_stage_logs",
            query,
            sort=[("timestamp", 1)]  # 按时间戳升序排序
        )

        # 构建日志列表
        log_list = []
        for log in logs:
            if "message" in log and "level" in log and "timestamp" in log:
                log_list.append({
                    "message": log["message"],
                    "loglevel": log["level"],
                    "timestamp": log["timestamp"]
                })
            
        # 获取最后一个日志的ID
        last_log_id = str(logs[-1]["_id"]) if logs else None

        return {
            "code": 200,
            "message": "获取日志成功",
            "data": {
                "logs": log_list,
                "last_log_id": last_log_id  # 返回最后一个_id
            }
        }

    except Exception as e:
        print(f"获取任务日志时出错: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"获取任务日志失败: {str(e)}"
        )

def query_group_return_analysis(task_id: str):
    """
    查询分组收益分析数据
    :param task_id: 任务ID
    :return: 分组收益分析数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = GroupReturnAnalysisResponse(
            task_id=task_id,
            group_return_analysis=result.get("group_return_analysis", [])
        )

        logger.info(f"成功查询到任务 {task_id} 的分组收益分析数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询分组收益分析数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询分组收益分析数据失败: {str(e)}")

def query_ic_decay_chart(task_id: str):
    """
    查询因子IC衰减图数据
    :param task_id: 任务ID
    :return: IC衰减图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = ICDecayChartResponse(
            task_id=task_id,
            ic_decay_chart=result.get("ic_decay_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的IC衰减图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询IC衰减图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询IC衰减图数据失败: {str(e)}")

def query_ic_density_chart(task_id: str):
    """
    查询因子IC分布图数据
    :param task_id: 任务ID
    :return: IC分布图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = ICDensityChartResponse(
            task_id=task_id,
            ic_den_chart=result.get("ic_den_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的IC分布图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询IC分布图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询IC分布图数据失败: {str(e)}")

def query_ic_self_correlation_chart(task_id: str):
    """
    查询因子IC自相关图数据
    :param task_id: 任务ID
    :return: IC自相关图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = ICSelfCorrelationChartResponse(
            task_id=task_id,
            ic_self_correlation_chart=result.get("ic_self_correlation_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的IC自相关图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询IC自相关图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询IC自相关图数据失败: {str(e)}")

def query_ic_sequence_chart(task_id: str):
    """
    查询因子IC序列图数据
    :param task_id: 任务ID
    :return: IC序列图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = ICSequenceChartResponse(
            task_id=task_id,
            ic_seq_chart=result.get("ic_seq_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的IC序列图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询IC序列图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询IC序列图数据失败: {str(e)}")

def query_rank_ic_decay_chart(task_id: str):
    """
    查询因子Rank IC衰减图数据
    :param task_id: 任务ID
    :return: Rank IC衰减图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = RankICDecayChartResponse(
            task_id=task_id,
            rank_ic_decay_chart=result.get("rank_ic_decay_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的Rank IC衰减图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询Rank IC衰减图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询Rank IC衰减图数据失败: {str(e)}")

def query_rank_ic_density_chart(task_id: str):
    """
    查询因子Rank IC分布图数据
    :param task_id: 任务ID
    :return: Rank IC分布图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = RankICDensityChartResponse(
            task_id=task_id,
            rank_ic_den_chart=result.get("rank_ic_den_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的Rank IC分布图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询Rank IC分布图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询Rank IC分布图数据失败: {str(e)}")

def query_rank_ic_self_correlation_chart(task_id: str):
    """
    查询因子Rank IC自相关图数据
    :param task_id: 任务ID
    :return: Rank IC自相关图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = RankICSelfCorrelationChartResponse(
            task_id=task_id,
            rank_ic_self_correlation_chart=result.get("rank_ic_self_correlation_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的Rank IC自相关图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询Rank IC自相关图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询Rank IC自相关图数据失败: {str(e)}")

def query_rank_ic_sequence_chart(task_id: str):
    """
    查询因子Rank IC序列图数据
    :param task_id: 任务ID
    :return: Rank IC序列图数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = RankICSequenceChartResponse(
            task_id=task_id,
            rank_ic_seq_chart=result.get("rank_ic_seq_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的Rank IC序列图数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询Rank IC序列图数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询Rank IC序列图数据失败: {str(e)}")

def query_last_date_top_factor(task_id: str):
    """
    查询最新日期的因子值数据
    :param task_id: 任务ID
    :return: 最新日期的因子值数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")
        result.get("last_date_top_factor", [])
        # 构造响应数据
        response = LastDateTopFactorResponse(
            task_id=task_id,
            last_date_top_factor=result.get("last_date_top_factor", [])
        )

        logger.info(f"成功查询到任务 {task_id} 的最新日期因子值数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询最新日期因子值数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询最新日期因子值数据失败: {str(e)}")

def query_one_group_data(task_id: str):
    """
    查询单组数据分析结果
    :param task_id: 任务ID
    :return: 单组数据分析结果
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = OneGroupDataResponse(
            task_id=task_id,
            one_group_data=result.get("one_group_data")
        )

        logger.info(f"成功查询到任务 {task_id} 的单组数据分析结果")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询单组数据分析结果失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询单组数据分析结果失败: {str(e)}")

def query_factor_excess_chart(task_id: str, resample: str = 'W'):
    """
    查询因子超额收益图表数据
    :param task_id: 任务ID
    :return: 超额收益图表数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = FactorExcessChartResponse(
            task_id=task_id,
            excess_chart=result.get("excess_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的超额收益图表数据")
        return ResultData.success(data=response.model_dump())

    except Exception as e:
        logger.error(f"查询超额收益图表失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询超额收益图表失败: {str(e)}")


def query_factor_analysis_data(task_id: str):
    """
    查询因子分析数据
    :param task_id: 任务ID
    :return: 因子分析数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = FactorAnalysisDataResponse(
            task_id=task_id,
            factor_data_analysis=result.get("factor_data_analysis", [])
        )

        logger.info(f"成功查询到任务 {task_id} 的因子分析数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询因子分析数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询因子分析数据失败: {str(e)}")


def query_return_chart(task_id: str):
    """
    查询因子收益率图表数据
    :param task_id: 任务ID
    :return: 收益率图表数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = ReturnChartResponse(
            task_id=task_id,
            return_chart=result.get("return_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的收益率图表数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询收益率图表数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询收益率图表数据失败: {str(e)}")

def query_simple_return_chart(task_id: str):
    """
    查询因子单组收益率图表数据
    :param task_id: 任务ID
    :return: 单组收益率图表数据
    """
    try:
        # 从数据库中查询结果
        result = _db_handler.mongo_find_one(
            "panda",
            "factor_analysis_results",
            {"task_id": task_id}
        )

        if not result:
            logger.warning(f"未找到任务 {task_id} 的分析结果")
            return ResultData.fail("404", f"未找到任务 {task_id} 的分析结果")

        # 构造响应数据
        response = SimpleReturnChartResponse(
            task_id=task_id,
            simple_return_chart=result.get("simple_return_chart")
        )

        logger.info(f"成功查询到任务 {task_id} 的单组收益率图表数据")
        return ResultData(
            code="200",
            message="查询成功",
            data=response.model_dump()
        )

    except Exception as e:
        logger.error(f"查询单组收益率图表数据失败: {str(e)}\n{traceback.format_exc()}")
        return ResultData.fail("500", f"查询单组收益率图表数据失败: {str(e)}")
    
    