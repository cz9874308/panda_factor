"""
请求体模型模块

本模块定义了 API 请求的数据模型，用于验证和解析客户端发送的请求数据。
使用 Pydantic 进行数据验证，确保请求数据格式正确。

核心概念
--------

- **请求模型**：定义 API 接口接收的请求参数结构
- **数据验证**：使用 Pydantic 自动验证请求参数的类型和格式
- **字段描述**：为每个字段提供描述和示例，自动生成 API 文档

为什么需要这个模块？
-------------------

在 Web API 开发中，需要验证客户端发送的请求数据：
- 确保必填字段存在
- 验证字段类型正确
- 验证字段值在有效范围内

这个模块通过 Pydantic 模型实现自动验证，减少手动验证代码。

注意事项
--------

- 所有模型都继承自 Pydantic 的 BaseModel
- 使用 Field 定义字段的元数据（示例、描述等）
- 使用 validator 定义自定义验证逻辑
"""

from pydantic import BaseModel, Field, validator
from datetime import date
from typing import Optional, Text
from panda_factor_server.models.common import Params


class CreateFactorRequest(BaseModel):
    """
    创建因子请求参数
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