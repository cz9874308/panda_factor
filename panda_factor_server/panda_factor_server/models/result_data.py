"""
统一响应结果模块

本模块定义了 API 的统一响应格式，所有 API 返回的数据都会包装在这个格式中。
这样可以让前端统一处理响应数据，方便错误处理和数据提取。

核心概念
--------

- **统一响应格式**：所有 API 返回相同的数据结构
- **状态码**：用于标识请求是否成功
- **消息**：用于描述请求结果或错误信息
- **数据**：实际返回的业务数据

为什么需要这个模块？
-------------------

在 Web API 开发中，统一的响应格式有很多好处：
- 前端可以统一处理响应数据
- 方便进行错误处理
- 提高 API 的一致性和可维护性

注意事项
--------

- 成功时使用 `ResultData.success()` 创建响应
- 失败时使用 `ResultData.fail()` 创建响应
- 支持泛型，可以指定 data 字段的类型
"""

from pydantic import BaseModel
from typing import Optional, TypeVar, Generic, Any

T = TypeVar('T')


class ResultData(BaseModel, Generic[T]):
    """统一响应结果类

    所有 API 返回的数据都会包装在这个格式中。

    Attributes:
        code: 状态码，"200" 表示成功，其他表示失败
        message: 描述信息
        data: 实际返回的业务数据，可选
    """
    code: str
    message: str
    data: Optional[T] = None

    @staticmethod
    def success(message: str = "success", data: Any = None) -> 'ResultData':
        return ResultData(
            code="200",
            message=message,
            data=data
        )

    @staticmethod
    def fail(code: str, message: str) -> 'ResultData':
        return ResultData(
            code=code,
            message=message,
            data=None
        )