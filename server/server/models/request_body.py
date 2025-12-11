"""
请求数据模型模块

本模块定义了 API 请求的数据模型，包括创建因子、更新因子等请求的数据结构。
使用 Pydantic 进行数据验证和序列化。

核心概念
--------

- **请求模型**：定义 API 请求的数据结构
- **数据验证**：使用 Pydantic 进行数据验证
- **字段描述**：提供详细的字段说明和示例

为什么需要这个模块？
-------------------

在 API 设计中，需要定义请求数据结构：
- 需要验证请求参数
- 需要提供字段说明
- 需要确保数据格式正确

这个模块提供了这些请求模型。

注意事项
--------

- 使用 Pydantic 进行数据验证
- 提供详细的字段描述和示例
- 支持可选字段和默认值
"""

from pydantic import BaseModel, Field, validator
from datetime import date
from typing import Optional, Text
from server.models.common import Params

class CreateFactorRequest(BaseModel):
    """创建因子请求模型

    这个类定义了创建因子请求的数据结构。

    Attributes:
        user_id: 用户 ID
        name: 因子中文名称
        factor_name: 因子英文名称（唯一）
        factor_type: 因子类型，'future' 或 'stock'
        is_persistent: 是否持久化，线上使用只传 false
        cron: Cron 表达式，开启持久化时传入
        factor_start_day: 因子持久化开始时间，开启持久化时传入
        code: 因子代码
        code_type: 代码类型，'formula' 或 'python'
        tags: 因子标签，多个标签用逗号分隔
        status: 状态，0:未运行，1:运行中，2:运行成功，3:运行失败
        describe: 因子描述
        params: 因子参数（可选）
    """
    user_id: str = Field(..., example="2", description="用户id")
    name: str=Field(..., example="圣杯", description="因子中文名称")
    factor_name: str = Field(..., example="Grail", description="Unique Factor English Name")
    factor_type: str = Field(..., example="macro", description="因子类型，只有两种：future｜stock")
    is_persistent: bool = Field(default=False, example=False, description="是否持久化，线上使用只传 false")
    cron: Optional[str] = Field(default=None, example="0 0 12 * * ?", description="cron表达式，开启持久化时传入，默认为null")
    factor_start_day: Optional[str] = Field(default=None, example="2018-01-01", description="因子持久化开始时间，开启持久化时传入，默认为null")
    code: Text = Field(..., example="json", description="代码")
    code_type: str = Field(..., example="formula", description="因子类型，只有两种：formula｜python")
    tags: str= Field(..., example="动量因子,质量因子", description="因子标签，多个标签\",\"分隔")
    status: int = Field(..., example=0, description="状态：0:未运行，1:运行中，2:运行成功，3：运行失败")
    describe: str = Field(..., example="该因子表述换手率因子", description="描述")
    params: Optional[Params] = Field(default=None, description="参数")

    # 添加验证器，将日期字符串转换为 ISO 格式
    @validator('factor_start_day')
    def validate_factor_start_day(cls, v):
        if v is not None:
            try:
                return date.fromisoformat(v).isoformat()
            except ValueError:
                raise ValueError('Invalid date format. Use YYYY-MM-DD')
        return v