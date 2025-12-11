"""
因子分析日志模型模块

本模块定义了因子分析日志的数据模型，就像一个"日志记录模板"，
它提供了标准化的日志数据结构，确保日志信息能够正确存储和查询。

核心概念
--------

- **FactorAnalysisLog**：因子分析日志模型，包含日志的所有信息

使用方式
--------

1. 创建 `FactorAnalysisLog` 实例，记录分析日志
2. 保存到 MongoDB 数据库
3. 后续可以查询和分析日志

工作原理
--------

就像记录实验日志：

1. **记录信息**：创建日志实例，包含所有相关信息（就像记录实验日志）
2. **保存日志**：将日志保存到数据库（就像将日志归档）
3. **查询分析**：后续可以查询和分析日志（就像查看历史记录）
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class FactorAnalysisLog(BaseModel):
    """因子分析日志模型：记录因子分析过程中的所有日志信息

    这个类就像一个"日志记录单"，它定义了因子分析日志的完整结构，
    包括任务信息、因子信息、日志级别、消息内容等。

    为什么需要这个类？
    --------------------

    在因子分析过程中，会产生大量的日志信息：
    - 分析进度
    - 数据处理状态
    - 错误和警告
    - 性能指标

    这些日志需要：
    - 结构化存储（方便查询）
    - 关联任务和因子（方便追踪）
    - 包含详细信息（方便调试）

    这个类提供了标准化的日志结构，解决了这些问题。

    工作原理
    --------

    就像记录实验日志：

    1. **基本信息**：记录任务 ID、因子 ID、用户 ID 等（就像记录实验基本信息）
    2. **日志内容**：记录日志级别、消息、时间戳等（就像记录实验过程）
    3. **详细信息**：记录分析阶段、额外详情等（就像记录实验细节）
    4. **存储查询**：保存到数据库，后续可以查询（就像归档实验记录）

    Attributes:
        log_id: 日志 ID，唯一标识一条日志
        task_id: 关联的任务 ID，用于关联分析任务
        factor_id: 因子 ID，用于关联因子
        factor_name: 因子名称，方便阅读
        user_id: 用户 ID，用于关联用户
        level: 日志级别，如 DEBUG、INFO、WARNING、ERROR
        message: 日志消息，具体的日志内容
        timestamp: 日志时间戳，记录日志产生的时间
        stage: 分析阶段，如 "data_loading"、"calculation" 等
        details: 额外详情字典，包含额外的调试信息

    Example:
        >>> log = FactorAnalysisLog(
        ...     log_id="f7f46e9b-0d4a-4f2c-9b5e-3f0e3f8c0d1a",
        ...     task_id="a1b2c3d4-e5f6-7890-abcd-1234567890ab",
        ...     factor_id="60f1a2b3c4d5e6f7g8h9i0j1",
        ...     factor_name="动量因子",
        ...     user_id="user123",
        ...     level="INFO",
        ...     message="开始进行因子数据清洗",
        ...     timestamp="2023-07-01T12:34:56.789Z",
        ...     stage="data_cleaning",
        ...     details={"rows_processed": 1000}
        ... )
    """
    log_id: str = Field(..., description="日志ID")
    task_id: str = Field(..., description="关联的任务ID")
    factor_id: str = Field(..., description="因子ID")
    factor_name: str = Field(..., description="因子名称")
    user_id: str = Field(..., description="用户ID")
    level: str = Field("INFO", description="日志级别：DEBUG, INFO, WARNING, ERROR")
    message: str = Field(..., description="日志消息")
    timestamp: str = Field(..., description="日志时间戳")
    stage: str = Field("default", description="分析阶段")
    details: Optional[Dict[str, Any]] = Field(None, description="额外详情")

    class Config:
        schema_extra = {
            "example": {
                "log_id": "f7f46e9b-0d4a-4f2c-9b5e-3f0e3f8c0d1a",
                "task_id": "a1b2c3d4-e5f6-7890-abcd-1234567890ab",
                "factor_id": "60f1a2b3c4d5e6f7g8h9i0j1",
                "factor_name": "动量因子",
                "user_id": "user123",
                "level": "INFO",
                "message": "开始进行因子数据清洗",
                "timestamp": "2023-07-01T12:34:56.789Z",
                "stage": "data_cleaning",
                "details": {"rows_processed": 1000, "rows_filtered": 50}
            }
        } 